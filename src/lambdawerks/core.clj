(ns lambdawerks.core
	(:require [lambdawerks.xml-handling :refer [read-xml-chunk xml-record-traversal]]
			 [lambdawerks.db-handling :refer [multi-select multi-insert multi-update offsets-for-select add-person-ids drop-person-ids]]
			 [lambdawerks.utilities :refer [log-stats stats-to-txt]]
			 [clj-time.coerce :as c]
			 [clojure.core.async
						 :as async
						 :refer [>!! <!! go chan close!]])
  (:gen-class))


(defn maps-transformation 
	"Takes a set of maps. Returns a map of maps with keys :fname :lname :dob 
	and values the phones."
	[maps] 
	(zipmap (map #(dissoc % :phone) maps) (map #(:phone %) maps)))  		
		
(defn crosscheck-records
	"Takes a set of maps representing records in the person table, an atom containing
	a set of maps representing records of the update file, and an atom containing a
	vector of maps representing the corrected records that need to be updated in the db.
	Checks if a part of the records of the update file exist in the db and it handles
	them appropriately. This function is used only for side effects and the result
	is thrown away."
	[db-records xml-record-holder update-repo]
	(let [db-maps (maps-transformation db-records)
		 wrong-phones (filter #(if-let [db-phone (db-maps (dissoc % :phone))] (not= db-phone (:phone %)) false) @xml-record-holder)]	            
		(apply swap! update-repo conj wrong-phones)
		(apply swap! xml-record-holder disj (apply conj 
											wrong-phones 
											(clojure.set/intersection @xml-record-holder db-records)))))	

(defn count-db-traversals
	"Takes two long numbers representing how many records are in the update file
	and how many records we will extract each time we read the update file. Returns
	the number of the total db traversals which we will need in order to complete the
	whole operation."
	[records-in-xml xml-batch-size]
	(Math/ceil (/ records-in-xml xml-batch-size)))		

(defn format-dates 
	"Takes a collection of maps representing records in the person table. Returns a vector
	of maps, where the date instances have been replaced by strings of the YYYY-MM-DD part only."
	[db-records]
	(mapv #(update % :dob (fn [date] (if date (.toString date) nil))) db-records))				
				
(defn check-repos 
	"Takes an atom containing a vector of maps which will be inserted to db, an atom containing a
	vector of maps representing the corrected records that need to be updated in the db, and a long
	number representing the point where the update and insertion repositories (atoms) must be emptied.
	Checks if the count of the repositories (atoms) have reached the repo-limit and inserts/updates
	the db with their stuff if it did. This function is used only for side effects and the result
	is thrown away."
	[insert-repo update-repo repo-limit]
	(dorun
		(map #(when (>= (count @%) repo-limit) 
					(%2 @%) 
					(reset! % [])) 
			 [insert-repo update-repo]
			 [multi-insert multi-update])))				

(defn empty-xml-record-holder 
	"Takes an atom containing a set of maps representing records of the update file.
	Empties the atom holding the records of a part of the update file. This function
	is used only for side effects and the result is thrown away."
	[xml-record-holder]
	(reset! xml-record-holder #{}))			 
			 
(defn fill-xml-record-holder 
	"Takes a collection of maps representing records in the update file and an
	atom containing a set of maps representing records of the update file. Fills
	the atom holding the records of a part of the update file with new records
	from the update file. This function is used only for side effects and the
	result is thrown away."
	[xml-record-holder xml-records]
	(apply swap! xml-record-holder conj xml-records))			 
			 
(defn archive-missing-records
	"Takes an atom containing a set of maps representing records of the update
	file and an atom containing a vector of maps representing records that need to be
	inserted to the db. Fills the repository holding the records which aren't found
	in the db in the previous db traversal. This function is used only for side
	effects and the result is thrown away."
	[xml-record-holder insert-repo]
	(apply swap! insert-repo conj @xml-record-holder))
			 
(defn update-db 
	"Orchestrates the whole db update operation."
	[]
	(let [records-in-db 10000000
		 records-in-xml 1500000
		 select-size 400000
		 xml-batch-size 100000
		 repo-limit 1000
		 db-traversals (count-db-traversals records-in-xml xml-batch-size)
		 db-offsets (offsets-for-select records-in-db select-size)
		 insert-repo (atom [])
		 update-repo (atom [])
		 xml-record-holder (atom #{})
		 load-stats (atom [])
		 xml-channel (chan)]
		 (log-stats load-stats "start")
		 (async/thread (read-xml-chunk xml-channel xml-batch-size))
		 (println "adding ids to person table")
		 (add-person-ids)
		 (println "ids set")
		(dotimes [iteration db-traversals]
				(log-stats load-stats (str "before " (inc iteration) " db traversal"))
				(archive-missing-records xml-record-holder insert-repo) ;insert stuff after a db complete traversal				
				(println "insert-repo count: " (count @insert-repo))
				(empty-xml-record-holder xml-record-holder) ;empty stuff from the previous full db traversal				
				(async/>!! xml-channel "Give next batch")
				(->> (async/<!! xml-channel)
					(mapv #(xml-record-traversal %))
					(fill-xml-record-holder xml-record-holder))
				(check-repos insert-repo update-repo repo-limit)	
				(doseq [db-offset db-offsets]
					(-> db-offset
					  (multi-select select-size)
					  (format-dates)
					  (set)
					  (crosscheck-records xml-record-holder update-repo))
					(println "offset: " db-offset ", update-repo count: " (count @update-repo) ", xml-holder count: " (count @xml-record-holder))))
		(archive-missing-records xml-record-holder insert-repo) ;after the last db traversal we have to insert the missing records in the repo
		(multi-insert @insert-repo)
		(multi-update @update-repo)
		(drop-person-ids)()
		(async/>!! xml-channel "End") ;send it in order to be able to stop waiting
		(async/close! xml-channel)
		(log-stats load-stats "end")
		(println "db update done")
		(stats-to-txt load-stats "load statistics.txt")))
  
(defn -main [& args]
  )
