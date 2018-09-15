(ns lambdawerks.utilities)

(defn altered-drop [s drop-num]
	(drop drop-num s))

(defn altered-take [s take-num]
	(take take-num s))	
	
(defn get-cores []
	(.availableProcessors (Runtime/getRuntime))
	1)
	
(defn work-sharing [tasks cores]
  (let [core-num cores
        tasks-num (count tasks)]
    (cond 
      (<= tasks-num core-num) (partition 1 tasks)
      (> tasks-num core-num) (if (> (mod tasks-num core-num) 0)
                               (let [initial-division (partition-all (quot tasks-num core-num) tasks)]
                                 (concat
                                   (drop (mod tasks-num core-num) (take core-num initial-division))
                                   (map conj initial-division (take-last (mod tasks-num core-num) tasks))))
                               (partition-all (quot tasks-num core-num) tasks)))))
	