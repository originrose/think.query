# think.query

A query language for clojure.

## think.model.query
The query system is a general purpose system for querying in-memory indexes
in addition to the datomic backing style.  The intent is to reduce the number of endpoints by providing a
general purpose language that can specify the shape of filtered, ordered sets of results.  It is composed
of two possible operations, either a query operator or a compute operator which are currently
implemented as multimethods.  The source data for the query system is a set of indexes defined near
the top of the file as well some ability to translate queries to the datomic backing store.
There are a couple base assumptions the system makes about the underlying data, the first being
that a query maps to a single resource type (currently either :resource.type/visual-variant
or :resource.type/brand) and that these types have valid resource id's

## think.model.query/let
The `let` operator allows the user to combine several queries into one result while allowing the user to control the shape of the response. The following example performs two queries and binds them to a map in the result.

```.clj
(query-user '[:let [a [:realize [:select {:user/username "alice@foo.com"}]]
                    b [:realize [:select {:user/username "bob@foo.com"}]]
                    c [:hydrate a [:user/email]]
                    d [:hydrate b [:user/first-name]]]
                  {:alice-email c :bob-name d}])
```
The result of such a let would look like the following:
```.clj
{:alice-email "alice@foo.com"
 :bob-name "Bob"}
```

## think.model.query/transform
The `tranform` operator allows a server-side transform to be performed on an arbitrary sequence of results. The tranform operator takes a data input as the result of another query operator as well as arbitrary arguments provided by the client. The following example would be provided on the server (Note that this tranform is better suited for a :hydrate, however it is provided here as means of an example how any function over the data could be performed):

```.clj
(defmethod q/transform-operator :email-list
  [_ data args]
  (map :user/email data))
```

Then, the client could use the `transform` operator as follows:
```.clj
(query-user [:transform [:realize [:select :*]] :email-list])]
```

Producing a result similar to the following: 
```.clj
["alice@foo.com" "bob@foo.com"]
```

