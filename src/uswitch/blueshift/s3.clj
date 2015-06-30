(ns uswitch.blueshift.s3
  (:require [com.stuartsierra.component :refer (Lifecycle system-map using start stop)]
            [clojure.tools.logging :refer (info error warn debug errorf)]
            [aws.sdk.s3 :refer (list-objects get-object delete-object put-object copy-object)]
            [clojure.set :refer (difference)]
            [clojure.core.async :refer (go-loop thread put! chan >!! <!! >! <! alts!! timeout close!)]
            [clojure.edn :as edn]
            [uswitch.blueshift.util :refer (close-channels clear-keys)]
            [schema.core :as s]
            [metrics.counters :refer (counter inc! dec!)]
            [metrics.timers :refer (timer time!)]
            [uswitch.blueshift.redshift :as redshift])
  (:import [java.io PushbackReader InputStreamReader]
           [org.apache.http.conn ConnectionPoolTimeoutException]))

(defrecord Manifest [table pk-columns columns jdbc-url options data-pattern strategy staging-select])

(def ManifestSchema {:table          s/Str
                     :pk-columns     [s/Str]
                     :columns        [s/Str]
                     :jdbc-url       s/Str
                     :strategy       s/Str
                     :options        s/Any
                     :staging-select (s/maybe (s/either s/Str s/Keyword))
                     :data-pattern   s/Regex
                     (s/optional-key :rename-prefix) s/Str
                     (s/optional-key :rename-suffix) s/Str
                     (s/optional-key :rename-files) s/Bool
                     (s/optional-key :direct-table-copy) s/Bool
                     })

(def errors-folder "processed-errors/")

(defn validate [manifest]
  (when-let [error-info (s/check ManifestSchema manifest)]
    (throw (ex-info "Invalid manifest. Check map for more details." error-info))))

(defn listing
  [credentials bucket & opts]
  (let [options (apply hash-map opts)]
    (loop [marker   nil
           results  nil]
      (let [{:keys [next-marker truncated? objects]}
            (list-objects credentials bucket (assoc options :marker marker))]
        (if (not truncated?)
          (concat results objects)
          (recur next-marker (concat results objects)))))))

(defn files [credentials bucket directory]
  (listing credentials bucket :prefix directory))

(defn directories
  ([credentials bucket]
     (:common-prefixes (list-objects credentials bucket {:delimiter "/"})))
  ([credentials bucket path]
     {:pre [(.endsWith path "/")]}
     (:common-prefixes (list-objects credentials bucket {:delimiter "/" :prefix path}))))

(defn leaf-directories
  [credentials bucket]
  (loop [work (directories credentials bucket)
         result nil]
    (if (seq work)
      (let [sub-dirs (directories credentials bucket (first work))]
        (recur (concat (rest work) sub-dirs)
               (if (seq sub-dirs)
                 result
                 (cons (first work) result))))
      result)))

(defn read-edn [stream]
  (edn/read (PushbackReader. (InputStreamReader. stream))))

(defn assoc-if-nil [record key value]
  (if (nil? (key record))
    (assoc record key value)
    record))

(defn manifest [credentials bucket files]
  (letfn [(manifest? [{:keys [key]}]
            (re-matches #".*manifest\.edn$" key))]
    (when-let [manifest-file-key (:key (first (filter manifest? files)))]
      (with-open [content (:content (get-object credentials bucket manifest-file-key))]
        (-> (read-edn content)
            (map->Manifest)
            (assoc-if-nil :strategy "merge")
            (update-in [:data-pattern] re-pattern))))))

(defn- get-filename-of-S3-file
  [file]
  (if (.contains file ".")
    (subs file 0 (.lastIndexOf file "."))
    file
  )
)

(defn- get-path-of-S3-file
  [file]
  (if (.contains file "/")
    (subs file 0 (+ 1 (.lastIndexOf file "/")))
    ""
  )
)

(defn- step-scan
  [credentials bucket directory]
  (try
    (let [fs (files credentials bucket directory)]
      (if-let [manifest (manifest credentials bucket fs)]        
        (do        
          (validate manifest)
          (let [data-files  (filter (fn [{:keys [key]}]
                                      (re-matches (:data-pattern manifest) key))
                                    fs)]
            (if (seq data-files)
              (do
                (info "Watcher triggering import" (:table manifest))
                (debug "Triggering load:" load)
                {:state :load, :table-manifest manifest, :files (map :key data-files)})
              {:state :scan, :pause? true})))
        {:state :scan, :pause? true}))
    (catch clojure.lang.ExceptionInfo e
      (error e "Error with manifest file")
      {:state :scan, :pause? true})
    (catch ConnectionPoolTimeoutException e
      (warn e "Connection timed out. Will re-try.")
      {:state :scan, :pause? true})
    (catch Exception e
      (error e "Failed reading content of" (str bucket "/" directory))
      {:state :scan, :pause? true})))

(def importing-files (counter [(str *ns*) "importing-files" "files"]))
(def import-timer (timer [(str *ns*) "importing-files" "time"]))

(defn- step-load
  [credentials bucket table-manifest files]
  (let [redshift-manifest  (redshift/manifest bucket files)
        {:keys [key url]}  (redshift/put-manifest credentials bucket redshift-manifest)]

    (info "Importing" (count files) "data files to table" (:table table-manifest) "from manifest" url)
    (debug "Importing Redshift Manifest" redshift-manifest)
    (inc! importing-files (count files))
    (try (time! import-timer
                (redshift/load-table credentials url table-manifest))
         (info "Successfully imported" (count files) "files")         
         
         ;;deletes the manifest created
         (delete-object credentials bucket key)
         (dec! importing-files (count files))                          

         ;;if rename-files = true in manifest config
         (if (:rename-files table-manifest) 
             {:state :move :files files :table-manifest table-manifest}
             {:state :delete :files files})

         
         (catch java.sql.SQLException e
           (error e "Error loading into" (:table table-manifest))
           (error (:table table-manifest) "Redshift manifest content:" redshift-manifest)
           
           ;;comment these three lines if testing and want same file re-processed.           
           (delete-object credentials bucket key)
           (dec! importing-files (count files))
  
           ;;if rename-files = true in manifest config
           (if (:rename-files table-manifest) 
               {:state :move :files files :table-manifest table-manifest :error true}
               {:state :delete :files files})))))

(defn- step-delete
  [credentials bucket files]
  (do
    (doseq [key files]
      (info "Deleting" (str "s3://" bucket "/" key))
      (try                
        (delete-object credentials bucket key)
        (catch Exception e
          (warn "Couldn't delete" key "  - ignoring"))))
    {:state :scan, :pause? true}))    

(defn- step-move  
  "optional renaming of a processed csv file, options are 
   rename-prefix and rename-suffix specified in the manifest.edn"
  [credentials bucket manifest files error]  
  (do
    ;;both rename-prefix rename-suffix = nil means file will be deleted after rename
    ;;since step-delete runs after rename
    (doseq [key files]
      (let [
            path (get-path-of-S3-file key)            
            filename (get-filename-of-S3-file (clojure.string/replace key path ""))
            rename-prefix (or (:rename-prefix manifest) path)
            rename-suffix (or (:rename-suffix manifest) (subs key (.lastIndexOf key ".")))            
            new-filename  (if error (str errors-folder filename rename-suffix) (str rename-prefix filename rename-suffix))
            ]
        (info "Renaming" "s3://" bucket "/" key  " to " "s3://" bucket new-filename)
        (try
          (copy-object credentials bucket key new-filename)
          (delete-object credentials bucket key)
        (catch Exception e 
          (error e "Error renaming file:" key)
        ))
      )      
    )
    {:state :scan, :pause? true}
  )
)

(defn- progress
  [{:keys [state] :as world}
   {:keys [credentials bucket directory] :as configuration}]
  
  (case state
    :scan   (step-scan   credentials bucket directory )
    :load   (step-load   credentials bucket                 (:table-manifest world) (:files world))    
    :delete (step-delete credentials bucket                 (:files world))
    :move (step-move credentials bucket                 (:table-manifest world) (:files world) (:error world))
  ))

(defrecord KeyWatcher [credentials bucket directory poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting KeyWatcher for" (str bucket "/" directory) "polling every" poll-interval-seconds "seconds")
    (let [control-ch    (chan)
          configuration {:credentials credentials :bucket bucket :directory directory}]
      (thread
       (loop [timer (timeout (* poll-interval-seconds 1000))
              world {:state :scan}]
         (let [next-world (progress world configuration)]
           (if (:pause? next-world)
             (let [[_ c] (alts!! [control-ch timer])]
               (when (not= c control-ch)
                 (recur (timeout (* poll-interval-seconds 1000)) next-world)))
             (recur timer next-world)))))
      (assoc this :watcher-control-ch control-ch)))
  (stop [this]
    (info "Stopping KeyWatcher for" (str bucket "/" directory))
    (close-channels this :watcher-control-ch)))


(defn spawn-key-watcher! [credentials bucket directory poll-interval-seconds]
  (start (KeyWatcher. credentials bucket directory poll-interval-seconds)))

(def directories-watched (counter [(str *ns*) "directories-watched" "directories"]))

(defrecord KeyWatcherSpawner [bucket-watcher poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting KeyWatcherSpawner")
    (let [{:keys [new-directories-ch bucket credentials]} bucket-watcher
          watchers (atom nil)]
      (go-loop [dirs (<! new-directories-ch)]
        (when dirs
          (doseq [dir dirs]
            (swap! watchers conj (spawn-key-watcher! credentials bucket dir poll-interval-seconds))
            (inc! directories-watched))
          (recur (<! new-directories-ch))))
      (assoc this :watchers watchers)))
  (stop [this]
    (info "Stopping KeyWatcherSpawner")
    (when-let [watchers (:watchers this)]
      (info "Stopping" (count @watchers) "watchers")
      (doseq [watcher @watchers]
        (stop watcher)
        (dec! directories-watched)))
    (clear-keys this :watchers)))

(defn key-watcher-spawner [config]
  (map->KeyWatcherSpawner {:poll-interval-seconds (-> config :s3 :poll-interval :seconds)}))

(defn matching-directories [credentials bucket key-pattern]
  (try (->> (leaf-directories credentials bucket)
            (filter #(re-matches key-pattern %))
            (set))
       (catch Exception e
         (errorf e "Error checking for matching object keys in \"%s\"" bucket)
         #{})))

(defrecord BucketWatcher [credentials bucket key-pattern poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting BucketWatcher. Polling" bucket "every" poll-interval-seconds "seconds for keys matching" key-pattern)
    (let [new-directories-ch (chan)
          control-ch         (chan)]
      (thread
        (loop [dirs nil]
          (let [available-dirs (matching-directories credentials bucket key-pattern)
                new-dirs       (difference available-dirs dirs)]
            (when (seq new-dirs)
              (info "New directories:" new-dirs "spawning" (count new-dirs) "watchers")
              (>!! new-directories-ch new-dirs))
            (let [[v c] (alts!! [(timeout (* 1000 poll-interval-seconds)) control-ch])]
              (when-not (= c control-ch)
                (recur available-dirs))))))
      (assoc this :control-ch control-ch :new-directories-ch new-directories-ch)))
  (stop [this]
    (info "Stopping BucketWatcher")
    (close-channels this :control-ch :new-directories-ch)))

(defn bucket-watcher
  "Creates a process watching for objects in S3 buckets."
  [config]
  (map->BucketWatcher {:credentials (-> config :s3 :credentials)
                       :bucket (-> config :s3 :bucket)
                       :poll-interval-seconds (-> config :s3 :poll-interval :seconds)
                       :key-pattern (or (re-pattern (-> config :s3 :key-pattern))
                                        #".*")}))

(defrecord PrintSink [prefix chan-k component]
  Lifecycle
  (start [this]
    (let [ch (get component chan-k)]
      (go-loop [msg (<! ch)]
        (when msg
          (info prefix msg)
          (recur (<! ch)))))
    this)
  (stop [this]
    this))

(defn print-sink
  [prefix chan-k]
  (map->PrintSink {:prefix prefix :chan-k chan-k}))

(defn s3-system [config]
  (system-map :bucket-watcher (bucket-watcher config)
              :key-watcher-spawner (using (key-watcher-spawner config)
                                          [:bucket-watcher])))
