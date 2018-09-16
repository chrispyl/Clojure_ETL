(ns lambdawerks.utilities)

(defn altered-drop 
	"Takes a sequence and a long number and applies these arguments 
	in the opposite order in the drop function."
	[s drop-num]
	(drop drop-num s))

(defn altered-take 
	"Takes a sequence and a long number and applies these arguments 
	in the opposite order in the take function."
	[s take-num]
	(take take-num s))	