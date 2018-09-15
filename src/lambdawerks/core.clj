(ns lambdawerks.core
	(:require [lambdawerks.xml-handling :refer [read-xml-chunk xml-record-traversal]]
			 [lambdawerks.db-handling :refer [a b v s multi-select multi-insert multi-update offsets-for-select str-to-date example-select-2]]
			 [lambdawerks.utilities :refer [get-cores work-sharing]]
			 [lambdawerks.load-statistics :refer [memory-usage cpu-usage]]
			 [clj-time.coerce :as c])
  (:gen-class))

;maps is a set containing maps
(defn maps-transformation [maps] 
	(zipmap (map #(dissoc % :phone) maps) (map #(% :phone) maps)))  		
		
(defn crosscheck-records [db-records xml-record-holder update-repo]
	;(Thread/sleep (* 1000 (rand-int 10)))
	;(println (java.util.Date.))
	;(println "waiting for check")
	(println "intersection " (count (clojure.set/intersection @xml-record-holder db-records)))
	(println "differencies " (count (clojure.set/difference @xml-record-holder db-records)))
	
	(let [xml-maps (maps-transformation @xml-record-holder)
		 db-maps (maps-transformation db-records)
		 wrong-phones (filter #(if-let [db-phone (db-maps (first %))] (not= db-phone (second %)) false) xml-maps)] ;wrong phones, so they go to update

		(dorun 
			(map #(swap! update-repo conj (assoc (first %) :phone (second %))) wrong-phones))		 
		(apply swap! xml-record-holder disj (apply conj 
											(mapv #(assoc (first %) :phone (second %)) wrong-phones) 
											(clojure.set/intersection @xml-record-holder db-records)))
	;(println "check done")
	;(println (java.util.Date.)
	)))
		
(defn distribute-work [db-records xml-record-holders update-repo]
	(doall
		(map #(future (crosscheck-records % %2 %3)) 
			(repeat db-records) 
			xml-record-holders
			(repeat update-repo))))	

(defn count-db-traversals [records-in-xml xml-batch-size]
	(Math/ceil (->> xml-batch-size
				(* (get-cores))
				(/ records-in-xml))))		

(defn format-dates [db-records]
	(mapv #(update % :dob (fn [old-val] (if old-val (.toString old-val) nil))) db-records))				

(defn extract-xml [iteration xml-batch-size]
	 (->>(read-xml-chunk 
			(* xml-batch-size (get-cores)) 
			(* iteration (* xml-batch-size (get-cores))))
		(mapv #(xml-record-traversal %))))
				
(defn check-repos [insert-repo update-repo repo-limit]
	(dorun
		(map #(when (>= (count @%) repo-limit) (do (%2 @%) (reset! % []))) 
			 [insert-repo update-repo]
			 [multi-insert multi-update])))				

(defn empty-xml-record-holders [xml-record-holders]
	(dorun
		(map #(reset! % #{}) xml-record-holders)))			 
			 
(defn fill-xml-record-holders [partitioned-xml-records xml-record-holders]
	(dorun
		(map #(apply swap! % conj %2) xml-record-holders partitioned-xml-records)))			 
			 
(defn archive-missing-records [xml-record-holders insert-repo]
	(dorun
		(map #(apply swap! insert-repo conj @%) xml-record-holders)))
			 
(defn update-db []
	(let [records-in-db 10000000
		 records-in-xml 1500000
		 select-size 400000
		 xml-batch-size 25000
		 repo-limit 1000
		 db-traversals (count-db-traversals records-in-xml xml-batch-size)
		 db-offsets (offsets-for-select records-in-db select-size)
		 insert-repo (atom [])
		 update-repo (atom [])
		 xml-record-holders (take (get-cores) (repeatedly #(atom #{})))]
		(dotimes [iteration db-traversals]
				(println "insert-repo: " (count @insert-repo))			
				(archive-missing-records xml-record-holders insert-repo) ;insert stuff after a db complete traversal				
				(empty-xml-record-holders xml-record-holders) ;from the previous full db traversal				
				(-> iteration
					(extract-xml xml-batch-size)
					(work-sharing (get-cores))
					(fill-xml-record-holders xml-record-holders)) 				
				(check-repos insert-repo update-repo repo-limit) ;check if repos have >1000 and empty them if true				
				(doseq [db-offset db-offsets]
					(let [futures (-> db-offset
									  (multi-select select-size)
									  (format-dates)
									  (set)
									  (distribute-work xml-record-holders update-repo))]
						 (println "offset: " db-offset ", update-repo: " (count @update-repo) ", xml-holder: " (count @(first xml-record-holders)))
						 (dorun
							(map deref futures)))))
		(archive-missing-records xml-record-holders insert-repo) ;after the last db traversal we have to insert the missing records in the repo
		(multi-insert @insert-repo)
		(multi-update @update-repo)))
  
  
(defn -main [& args]
  )
