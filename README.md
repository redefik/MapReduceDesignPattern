 **MapReduceDesignPattern**
 
 Questa è una raccolta di programmi Java che implementano alcuni design pattern MapReduce.
 
 * Average Word Length: esempio base
 * Better Average Word Length: esempio di Combiner Optimization e definizione di tipi di output custom
 * Word Partition: esempio base di funzione di partizionamento personalizzata, settaggio del numero di reducer, 
  uso di NullWritable per evitare che il reducer stampi un valore di output
 * Word Grep: esempio d'uso di setup() nel mapper per leggere un parametro fornito dall'utente 
 (l'espressione regolare in questo caso) 
 * Word Distinct: pattern Distinct
 * Post Comment Import: esempio d'uso di MultipleInputs per utilizzare un mapper diverso a seconda della sorgente dati.
 * Word Sorting: esempio d'uso del campionamento per un'equa distribuzione delle chiavi intermedie ed esempio d'uso della
   classe SequenceFile per i file di piccole dimensioni.
   