;; Sketch of support for nesting of clauses in cascalog queries

(ns cascalog.nested-queries
  (:use cascalog.api))

(defn casca-var?
  "Returns logical true if the item given looks like a cascalog variable."
  [v]
  (let [[ch1 ch2] (name v)]
    (or (= \? ch1)
        (= [\! \!] [ch1 ch2]))))

(defn clause-early-vars
  "Return elements of `clause` which look like cascalog variables and are
   not after an ouput indicator such as `:>`."
  [clause]
  (filter casca-var? (take-while (complement #{:> :>>}) clause)))

(defn clause-late-vars
  "Return elements of `clause` which look like cascalog variables and are
   after an ouput indicator such as `:>` if such is present."
  [clause]
  (let [[pre post] (split-with (complement #{:> :>>}) clause)
        late-part (or post pre)]
    (filter casca-var? late-part)))

(defn clause? [expr]
  (and (list? expr)
       (clause-early-vars expr)))

;; if we dispense with output vars, we can concisely write

'(<-q [in1 in2 sim]
      [in1-str in2-str] (distinct-pairs)
      [in1 in2]         (unjsonify ?in1-str ?in2-str)
      [sim]             (comparator-fn ?in1 ?in2 {:locale :en})
      []                (meaningful-similarity? ?sim)
      [trap-tap]        :trap
      []                (:distinct false))

'(<-
    [?input1 ?input2 ?similarity]
    (distinct-pairs ?input1-str ?input2-str)
    (unjsonify ?input1-str ?input2-str :> ?input1 ?input2)
    (comparator-fn ?input1 ?input2 {:locale :en} :> ?similarity)
    (meaningful-similarity? ?similarity)
    (:trap trap-tap)
    (:distinct false))

(defn unpack-source-inside
  "A variant of unpacking a nested query clause which assumes \"source\"
   clauses are on the outside. Eg:

    (unpack-source-inside
      '(meaningful-similarity?
         (comparator-fn
           (unjsonify (distinct-pairs ?input1-str ?input2-str)
           :> ?input1 ?input2) {:locale :en}
         :> ?similarity)))
    ; =>
    '((distinct-pairs ?input1-str ?input2-str)
      (unjsonify ?input1-str ?input2-str :> ?input1 ?input2)
      (comparator-fn ?input1 ?input2 {:locale :en} :> ?similarity)
      (meaningful-similarity? ?similarity))
  [expr]"
  (loop [subexprs [], expr-out [], [e & etail :as expr] expr]
    (if expr
        (if (clause? e)
          (recur (conj subexprs e)
                 (vec (concat expr-out (expr-vars e)))
                 etail)
          (recur subexprs
                 (conj expr-out e)
                 etail))
        (cons (seq expr-out)
              (mapcat unpack subexprs)))))

(defn unpack-source-outside
  "A variant of unpacking a nested query clause which assumes \"source\"
   clauses are on the outside. Eg:

    (unpack-source-outside
      '(distinct-pairs
         (unjsonify ?input1-str ?input2-str :>
                    (comparator-fn ?input1 ?input2 {:locale :en} :>
                                   (meaningful-similarity? ?similarity)))))
    ; =>
    '((distinct-pairs ?input1-str ?input2-str)
      (unjsonify ?input1-str ?input2-str :> ?input1 ?input2)
      (comparator-fn ?input1 ?input2 {:locale :en} :> ?similarity)
      (meaningful-similarity? ?similarity))"
  [expr]
  (loop [subexprs [], expr-out [], [e & etail :as expr] expr]
    (if expr
        (if (clause? e)
          (recur (conj subexprs e)
                 (vec (concat expr-out (clause-early-vars e)))
                 etail)
          (recur subexprs
                 (conj expr-out e)
                 etail))
        (cons (seq expr-out)
              (mapcat unpack subexprs)))))

(defn unpack [expr]
  (if (list? expr)
    (if (#{'<- '?<- '<<-} (first expr))
      (mapcat unpack expr)
      (unpack-source-outside expr))
    [expr]))

(defmacro munpack [& exprs]
  `(mapcat unpack (quote ~exprs)))

(defmacro <-* [& exprs]
 `(<- ~@(mapcat unpack exprs)))

;; (munpack (a (b ?i) :> ?c))
;; ((a ?i :> ?c) (b ?i))



(= '(<-
    [?input1 ?input2 ?similarity]
    (distinct-pairs ?input1-str ?input2-str)
    (unjsonify ?input1-str ?input2-str :> ?input1 ?input2)
    (comparator-fn ?input1 ?input2 {:locale :en} :> ?similarity)
    (meaningful-similarity? ?similarity)
    (:distinct false))
   (munpack
    (<-
     [?input1 ?input2 ?similarity]
     (distinct-pairs
      (unjsonify ?input1-str ?input2-str :>
                 (comparator-fn ?input1 ?input2 {:locale :en} :>
                                (meaningful-similarity? ?similarity))))
     (:distinct false))))