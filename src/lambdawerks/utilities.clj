(ns lambdawerks.utilities
	(:require [lambdawerks.load-statistics :refer [memory-usage cpu-usage]]))

(defn stats-to-txt 
	"Takes an atom containing a vetor of maps and a string representing the name
	of a file. Writes the contents of the maps to the file."
	[stats-holder txt-name]
	(doseq [load-map @stats-holder]
		(spit txt-name (str "Heap utilization: " (load-map :heap-util)
									 " Used heap: " (load-map :used-heap)
									 " Max heap: " (load-map :max-heap)
									 " CPU utilization: " (load-map :cpu-util)
									 " CPU cores: " (load-map :cpu-num)
									 " CPU load average: " (load-map :cpu-load-average)
									 " Moment of measurement: " (load-map :when)		
									 (System/lineSeparator)) :append true)))
	
(defn create-load-map
	"Takes a string displaying 'when' the load measurement will take place.
	Returns a map with statistics about memory and CPU."
	[s]
	(let [[heap-util used-heap max-heap] (memory-usage)
		 [cpu-util cpu-num cpu-load-average] (cpu-usage)]
		{:heap-util heap-util :used-heap used-heap :max-heap max-heap
		 :cpu-util cpu-util :cpu-num cpu-num :cpu-load-average cpu-load-average :when s}))
	
(defn log-stats
	"Takes an atom containing a collection of maps, and a string displaying
	'when' the load measurement will take place. Inserts a load map to this atom.
	This function is used only for side effects and the result is thrown away."
	[stats-holder s]
	(swap! stats-holder conj (create-load-map s)))			

(defn flip 
	"Takes a function. Returns a function performing the same operation as the input
	function but which accepts its arguments in reverse order."
	[f]
  (fn [& xs]
    (apply f (reverse xs))))