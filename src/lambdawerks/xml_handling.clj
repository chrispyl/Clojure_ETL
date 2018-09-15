(ns lambdawerks.xml-handling
	(:require [clojure.data.xml :as xml]
			 [clojure.java.io :as io]
			 [lambdawerks.utilities :refer [altered-drop altered-take]]))
			 
			 
(defn read-xml-chunk [take-num drop-num]
	(with-open [rdr (clojure.java.io/reader "C:/Users/Christos/Desktop/update-file.xml")]
		(-> rdr
			(xml/parse)
			(:content)
			(altered-drop drop-num)
			(altered-take take-num)
			(vec))))

(defn xml-record-traversal [xml-record]			
	(let [[first-name last-name dob phone] (map #(first (:content %)) (:content xml-record))]
		{:fname first-name :lname last-name :dob dob :phone phone}))
