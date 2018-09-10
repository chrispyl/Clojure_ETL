(ns lambdawerks.xml-handling
	(:require [clojure.data.xml :as xml]
			 [clojure.java.io :as io]))
			 
			 
(defn read-xml []
	(with-open [rdr (clojure.java.io/reader "C:/Users/Christos/Desktop/small.xml")]
		(let [tree (xml/parse rdr)]
	
			(doseq [member-record (:content tree)]
				(let [[first-name last-name dob phone] (mapv #(first (:content %)) (:content member-record))]
					(println first-name last-name dob phone)))))) 			 