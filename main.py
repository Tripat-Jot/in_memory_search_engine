from typing import Any, List, Dict, Set, Optional



class Document:
    def __init__(self, doc_id : int, content : str, metaData : Dict[str, str]) -> None:
        self.doc_id = doc_id 
        self.content = content.lower()  # Converting to lower for case-insensitive 
        self.meta_data = metaData
    
    def get_key_value (self, key : str) -> str:
        return self.meta_data.get(key, "")
    
    def __str__(self) -> str:
        return f"{self.content} | Metadata: {self.meta_data} | id { self.doc_id}"




#-------------Search method implementation
class InvertedIndex:
    def __init__(self) -> None:
        self.index : Dict [str, Set[int]] = {}
    
    def addDocument (self, doc_id : int, content : str):
        # split the word to insert in index 
        words = content.lower().split()
        for word in words:
            if word not in self.index:
                self.index[word] = set()  # Single word can be present in multiple documents
            self.index[word].add(doc_id)
   
    def search(self, search_words : List[str]) -> Set[int]:
        # check search words 
        if not search_words:
            return set()
        # set of documents containing the first word
        result_set = self.index.get(search_words[0], set()).copy()

        # intersect with the set of documents containing the rest of the words
        for word in search_words[1:]:
            result_set.intersection_update(self.index.get(word, set()))
        
        return result_set

#------------Dataset --------------

class Dataset:
    def __init__(self) -> None:
        self.documents : Dict[int, Document] = {}
        self.inverted_index = InvertedIndex()
    
    def addDocument(self, doc_id: int, document : Document):
        self.documents[doc_id] = document
        self.inverted_index.addDocument(doc_id, document.content)
    
    def gte_document_by_id (self, doc_id : int) -> Document:
        return self.documents[doc_id]
    

# Factory Design Pattern 
class DatasetFactory:
    def createDataset(self) -> Dataset:
        return Dataset()

#------------ Sorting  --------------

# Strategy Pattern 
class SortStrategy:
    def sort (self, results: List['SearchResult'], key : str) -> List['SearchResult']:
        raise NotImplementedError("Sort strategy must  implemented sort method")

class KeySortStrategy(SortStrategy):
    def sort(self, results: List['SearchResult'], inp_key: str) -> List['SearchResult']:
        # print(results[0].document.get_key_value(inp_key))
        print(results[0].document)
        return sorted(results, key = lambda x : x.document.get_key_value(inp_key))



class SearchResult:
    def __init__(self, document : Document ) -> None:
        self.document = document
    
    # def __str__(self) -> str:
    #     return f"{self.document.content}"

class SingletonMeta(type):
    _instances = {}
    
    def __call__(cls, *args: Any, **kwds: Any) :
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwds)
            cls._instances[cls] = instance
        
        return cls._instances[cls]



class SearchEngine(metaclass = SingletonMeta):
    def __init__(self) -> None:
        self.datasets : Dict [str, Dataset] = {} # name , dataset
        self.sort_strategy : SortStrategy = KeySortStrategy()
        self.next_doc_id = 1 
    
    def create_dataset(self, name : str):
        if name not in self.datasets:
            factory = DatasetFactory()
            self.datasets[name] = factory.createDataset()
        
    def insert_document(self, dataset_name : str, content : str, metaData : Dict[str, str]):
        if dataset_name in self.datasets:
            doc = Document(self.next_doc_id, content, metaData)
            self.datasets[dataset_name].addDocument(self.next_doc_id, doc)
            self.next_doc_id +=1
        else :
            raise ValueError(f"Dataset '{dataset_name} does not exists.")

    def search(self, dataset_name, search_pattens, order_by_key : Optional[str] ):
        if dataset_name not in self.datasets:
            raise ValueError(f"Dataset '{dataset_name} does not exists.")
        
        search_words = search_pattens.lower().split()
        dataset = self.datasets[dataset_name]

        matching_doc_ids = dataset.inverted_index.search(search_words)
        print(matching_doc_ids)

        results = []

        for doc_id in matching_doc_ids:
            document = dataset.gte_document_by_id(doc_id)
            results.append(SearchResult(document))
        
        # if order_by_key:
        #     results = self.sort_strategy.sort(results, order_by_key)
        
        return results
        


if __name__ == "__main__":
    search_engine = SearchEngine()

    search_engine.create_dataset('blogs')

    # search_engine.insert_document( "blogs", "This is my first blog about design pattern", {"date" : "2024-08-20", "author" : "Alice"})
    # search_engine.insert_document( "blogs", "Design Pattern are important for software development", {"date" : "2024-08-20", "author" : "Bob"})

    results1 = search_engine.search("blogs","design pattern", order_by_key="date")
    for result in results1:
        print(result.document.doc_id)

