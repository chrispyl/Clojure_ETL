(ns lambdawerks.xml-handling
	(:require [clojure.data.xml :as xml]
			 [clojure.java.io :as io]
			 [lambdawerks.utilities :refer [flip take-drop]]
			 [clojure.core.async
						 :as async
						 :refer [>!! <!! go chan close!]]))
		 
(defn read-xml-chunk
	"Takes a channel which will be used for the communication with the main thread of execution
	and a long number representing how many records we want it send back each time. It accepts messages
	from the main thread of execution about sending the next batch of xml records."
	[xml-channel xml-batch-size]
	(with-open [rdr (clojure.java.io/reader "C:/Users/Christos/Desktop/update-file.xml")]
		(let [tree (xml/parse rdr)
			 content (:content tree)]
			(loop [xml-records content]
				(let [message (async/<!! xml-channel)]
					(when (= message "Give next batch")
						(let [[records rest-records] (take-drop xml-batch-size xml-records)] 
							(async/>!! xml-channel records)
							(recur rest-records))))))))

(defn xml-record-traversal 
	"Takes an Element representing an xml record. Returns this record as a map."
	[xml-record]			
	(let [[first-name last-name dob phone] (map #(first (:content %)) (:content xml-record))]
		{:fname first-name :lname last-name :dob dob :phone phone}))

		