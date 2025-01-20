# Parse&Furious

Repo to ingest documents into a graph db

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Introduction

`parse-furious` is a Python-based project designed to ingest documents into a graph database. It supports loading and processing PDF documents, cleaning them, and constructing hierarchical structures from the content.

## Features

- Load and split PDF documents into smaller chunks.
- Clean PDF documents by removing non-UTF-8 characters or broken content.
- Construct hierarchical structures from document content.
- Support for different PDF processing providers.

## Installation

To install the required dependencies, run:

```sh
pip install -r requirements.txt
```

## Usage

### Loading PDF Documents
To load and split PDF documents, use the **_PdfLoader_** class:
    
    ```python
    from pdf_loader import PdfLoader

    files = ["document1.pdf", "document2.pdf"]
    pdf_loader = PdfLoader(files)
    documents = pdf_loader.load_pdf_documents()
    ```
### Cleaning PDF Documents
To clean a PDF document, use the **_clean_pdf_** method:

    ```python
    from pdf_loader import clean_pdf

    cleaned_file = clean_pdf("document.pdf")
    ```

### Building Hierarchical Structures
To build a hierarchical structure from document content, use the _**build_hierarchical_structure_json**_ function:

    ```python
    from pdf_loader import build_hierarchical_structure_json
    
    data = [
        {"sentences": ["Header 1"], "level": 0, "tag": "header"},
        {"sentences": ["Paragraph 1"], "level": 1, "tag": "para"}
    ]
    structure = build_hierarchical_structure_json(data)
    ```
## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for details.