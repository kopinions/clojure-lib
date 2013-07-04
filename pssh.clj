(ns ssh.pssh
  (:import com.jcraft.jsch.JSch)
  (:import com.jcraft.jsch.Util)
  (:import java.util.Properties)
  (:use [clojure.contrib.duck-streams :only [reader]])
  (:use [clojure.pprint])
;  (:use [clojure.string :only [replace-first split upper-case trim-newline join]])
  (:use clojure.core)
;  (:use clojure.contrib.def)
; (:gen-class)
 )

(defrecord cmd-result [host exit stdout stderr])
(defrecord host-info [host port user passwd])


(defmacro with-session-with-port [session user password host port & body]
  "This macro creates a ssh session that is valid within it's scope."
  `(let [~session (doto (.getSession (new com.jcraft.jsch.JSch) ~user ~host ~port)
                         (.setConfig (doto (new java.util.Properties)  (.put "StrictHostKeyChecking" "no")))
                         (.setPassword ~password)
                         (.connect 2000))]
     (try
       (do ~@body)
       (finally (.disconnect ~session)))))

(defmacro with-session [user password host & body]
  "This macro creates a ssh session that is valid within it's scope, using the
default port 22 to connect."
  `(with-session-with-port ~user ~password ~host 22 ~@body))

(defn exec
  "Executes a command on the remote host and returns a seq of the lines the
command retured."
  [session command]
  (let
    [channel (.openChannel session "exec")]
    (doto channel
      (.setCommand command)
      (.setInputStream nil)
      (.setOutputStream System/out)
      (.setErrStream System/err))
    (with-open
      [stream (.getInputStream channel) errstream (.getExtInputStream channel)]
      (.connect channel 2000)
      (let [retcode (.getExitStatus channel)]
        {:host (.getHost session) :exit  retcode :stdout (doall (line-seq (clojure.contrib.duck-streams/reader stream))) :stderr (doall (line-seq (clojure.contrib.duck-streams/reader errstream)))}))
    ))


(defn- ^cmd-result run-cmd [res h-info cmd]
  (with-session-with-port session (str (:user h-info)) (str (:passwd h-info)) (str (:host h-info)) (:port h-info) (map->cmd-result (exec session cmd))))

(defn execute [^host-info hi-list command]
  (let [results (doall (map #(agent (map->cmd-result {:host (:host %)})) hi-list))]
    (doseq [result results]
      (let [hi (first (filter #(= (:host %) (:host (deref result))) hi-list))]
        (send-off result run-cmd hi command)))
    (apply await-for 15000 results)
    (doall (map #(deref %) results))
    ))



;    (with-open [out-data (clojure.java.io/writer "dataaaaa")]
;      (.write  out-data (str (execute serverlist "ls /tmp"))))))
