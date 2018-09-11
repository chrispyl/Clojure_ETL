(ns lambdawerks.utilities)

(defn get-cores []
	(.availableProcessors (Runtime/getRuntime)))