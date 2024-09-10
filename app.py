from flask import Flask, request, jsonify, render_template
import csv
from io import StringIO
from typing import Any, List, Dict, Set, Optional
import json

# Search Engine implementation
class Document:
    def __init__(self, doc_id: int, content: str, metaData: Dict[str, str]) -> None:
        self.doc_id = doc_id
        self.content = content.lower()
        self.meta_data = metaData

    def get_key_value(self, key: str) -> str:
        return self.meta_data.get(key, "")


class InvertedIndex:
    def __init__(self) -> None:
        self.index: Dict[str, Set[int]] = {}

    def addDocument(self, doc_id: int, content: str):
        words = content.lower().split()
        for word in words:
            if word not in self.index:
                self.index[word] = set()
            self.index[word].add(doc_id)

    def search(self, search_words: List[str]) -> Set[int]:
        if not search_words:
            return set()
        result_set = self.index.get(search_words[0], set()).copy()
        for word in search_words[1:]:
            result_set.intersection_update(self.index.get(word, set()))
        return result_set


class Dataset:
    def __init__(self) -> None:
        self.documents: Dict[int, Document] = {}
        self.inverted_index = InvertedIndex()

    def addDocument(self, doc_id: int, document: Document):
        self.documents[doc_id] = document
        self.inverted_index.addDocument(doc_id, document.content)

    def gte_document_by_id(self, doc_id: int) -> Document:
        return self.documents[doc_id]
    


class DatasetFactory:
    def createDataset(self) -> Dataset:
        return Dataset()


class SortStrategy:
    def sort(self, results: List['SearchResult'], key: str) -> List['SearchResult']:
        raise NotImplementedError("Sort strategy must implement sort method")


class KeySortStrategy(SortStrategy):
    def sort(self, results: List['SearchResult'], inp_key: str) -> List['SearchResult']:
        return sorted(results, key=lambda x: x.document.get_key_value(inp_key))


class SearchResult:
    def __init__(self, document: Document) -> None:
        self.document = document


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args: Any, **kwds: Any):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwds)
            cls._instances[cls] = instance
        return cls._instances[cls]


class SearchEngine(metaclass=SingletonMeta):
    def __init__(self) -> None:
        self.datasets: Dict[str, Dataset] = {}
        self.sort_strategy: SortStrategy = KeySortStrategy()
        self.next_doc_id = 1

    def create_dataset(self, name: str):
        if name not in self.datasets:
            factory = DatasetFactory()
            self.datasets[name] = factory.createDataset()

    def insert_document(self, dataset_name: str, content: str, metaData: Dict[str, str]):
        if dataset_name in self.datasets:
            doc = Document(self.next_doc_id, content, metaData)
            self.datasets[dataset_name].addDocument(self.next_doc_id, doc)
            self.next_doc_id += 1
        else:
            raise ValueError(f"Dataset '{dataset_name}' does not exist.")

    def search(self, dataset_name: str, search_patterns: str, order_by_key: Optional[str]):
        if dataset_name not in self.datasets:
            raise ValueError(f"Dataset '{dataset_name}' does not exist.")

        search_words = search_patterns.lower().split()
        dataset = self.datasets[dataset_name]

        matching_doc_ids = dataset.inverted_index.search(search_words)
        results = [SearchResult(dataset.gte_document_by_id(doc_id)) for doc_id in matching_doc_ids]

        if order_by_key:
            results = self.sort_strategy.sort(results, order_by_key)

        return results


app = Flask(__name__)

search_engine = SearchEngine()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/create_dataset', methods=['POST'])
def create_dataset():
    dataset_name = request.form.get('dataset_name')
    if not dataset_name:
        return jsonify({'error': 'Dataset name is required'}), 400
    try:
        search_engine.create_dataset(dataset_name)
        # print(search_engine.datasets)
        return jsonify({'message': f"Dataset '{dataset_name}' created successfully"}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload_document', methods=['POST'])
def upload_document():
    dataset_name = request.form.get('dataset_name')
    content = request.form.get('content')
    metadata = request.form.get('metaData')
    
    if not dataset_name or dataset_name not in search_engine.datasets:
        return jsonify({'error': 'Dataset does not exist'}), 400

    if not content or not metadata:
        return jsonify({'error': 'Content and metadata are required'}), 400

    try:
        meta_data_dict = dict(item.split(':') for item in metadata.split(','))
        search_engine.insert_document(dataset_name, content, meta_data_dict)
        return jsonify({'message': 'Document uploaded successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload_bulk_documents', methods=['POST'])
def upload_bulk_documents():
    dataset_name = request.form.get('dataset_name')
    
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']

    print(file.filename)
    if not dataset_name:
        return jsonify({'error': 'Dataset name is required'}), 400
    else:
        search_engine.create_dataset(dataset_name)
        print(f"{dataset_name} dataset creation successful")

    try:
        stream = StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.reader(stream)
        print(csv_reader)


        # Process each row
        for row in csv_reader:
            if not row or len(row) < 2:
                continue

            content = row[0]
            metadata_str = row[1]

            # Check if the content is empty
            if not content.strip():
                continue

            str_test = "{"
            for index, item1 in enumerate(metadata_str.split(',')):
                if ':' in item1:
                    key, value = item1.split(':', 1)  # Splitting into key and value
                    str_test += f'"{key.strip()}": "{value.strip()}"'
                    if index < len(metadata_str.split(',')) - 1:
                        str_test += ', '
                else:
                    print(f"Skipping invalid item: {item1}")
            str_test += '}'
            print(str_test)
            search_engine.insert_document(dataset_name, content, str_test)

        return jsonify({'message': 'Bulk documents uploaded successfully!'}), 200


    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/search', methods=['GET'])
def search():
    dataset_name = request.args.get('dataset_name')
    search_patterns = request.args.get('search_patterns')
    order_by_key = request.args.get('order_by_key')
    print(order_by_key)
    
    if not dataset_name or dataset_name not in search_engine.datasets:
        return jsonify({'error': 'Dataset does not exist'}), 400
    
    # temp = search_engine.datasets
    # for item in temp:
    #     print(item)
    
    try:
        results = search_engine.search(dataset_name, search_patterns, order_by_key)
        response = [{'content': result.document.content, 'metadata': result.document.meta_data} for result in results]
        return jsonify({'results': response}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    

if __name__ == '__main__':
    app.run(debug=True)
