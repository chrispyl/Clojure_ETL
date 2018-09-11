(ns lambdawerks.xml-handling
	(:require [clojure.data.xml :as xml]
			 [clojure.java.io :as io]))
			 
			 
(defn read-xml [take-num drop-num]
	(with-open [rdr (clojure.java.io/reader "C:/Users/Christos/Desktop/update-file.xml")]
		(let [tree (xml/parse rdr)]
			(vec (take take-num (drop drop-num (:content tree)))))))

(defn xml-record-batch-traversal [xml-records]			
	(doseq [xml-record xml-records]
				(let [[first-name last-name dob phone] (mapv #(first (:content %)) (:content xml-record))]
					;{:fname first-name :lname last-name :dob dob :phone phone}
					;(println first-name last-name dob phone)
					;doseq returns nil always, watch that
					)))