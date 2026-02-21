# Unstructured.io Setup Guide

This guide explains how to set up Unstructured.io for high-quality PDF processing.

## Quick Start (Basic)

For basic functionality:

```bash
pip install "unstructured[pdf]"
```

This will work but uses a simpler "fast" strategy for partitioning.

## Recommended Setup (Better Quality)

For production-quality PDF processing with structure detection:

### 1. Install System Dependencies

**macOS:**
```bash
brew install tesseract poppler
```

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y tesseract-ocr poppler-utils
```

**Windows:**
- Tesseract: https://github.com/UB-Mannheim/tesseract/wiki
- Poppler: https://blog.alivate.com.au/poppler-windows/

### 2. Install Python Package with Local Inference

```bash
pip install "unstructured[local-inference]"
```

This enables the high-resolution ("hi_res") partitioning strategy which:
- Detects document layout using ML models
- Properly identifies titles, paragraphs, lists, tables
- Maintains hierarchical structure
- Much better results for technical books

## Verify Installation

```python
from unstructured.partition.pdf import partition_pdf

# Test basic partitioning
elements = partition_pdf(
    filename="test.pdf",
    strategy="auto"  # Will use "hi_res" if local-inference installed
)

print(f"Detected {len(elements)} elements")
for elem in elements[:5]:
    print(f"{elem.category}: {elem.text[:100]}...")
```

## How It Differs from pdfplumber

**pdfplumber:**
- Extracts raw text from PDF
- No structure awareness
- You manually parse section headers
- Simple but limited

**Unstructured.io:**
- Uses ML models to detect document structure
- Automatically identifies: Title, NarrativeText, ListItem, Table, etc.
- Preserves hierarchical relationships
- Production-grade, used by many companies

## Chunking Strategy

The script uses `chunk_by_title()` which:

1. **Preserves section boundaries** - Never splits across chapters/sections
2. **Combines related elements** - Groups paragraphs under same heading
3. **Maintains context** - Adds overlap between chunks
4. **Respects size limits** - Hard max at 2000 chars, soft max at 1800

Example:
```
Chapter 5: Replication
├── Paragraph 1: Leaders and Followers (500 chars)
├── Paragraph 2: Synchronous vs Async (600 chars)  
├── Paragraph 3: Handling Failures (700 chars)
└── [All combined into one chunk with section context]
```

## Performance Tips

1. **Use "hi_res" strategy** for best quality (requires local-inference)
2. **Adjust chunk size** based on your embedding model's context window
3. **Enable table detection** with `infer_table_structure=True`
4. **Monitor chunk distribution** to ensure even sizes

## Troubleshooting

**"No module named 'unstructured'"**
```bash
pip install "unstructured[pdf]"
```

**"tesseract is not installed or it's not in your PATH"**
- Install tesseract system package (see above)
- Verify: `tesseract --version`

**"Unable to get page count for..."**
- Install poppler: see system dependencies above
- Verify: `pdftoppm -v`

**Slow performance**
- First run downloads ML models (one-time)
- Use strategy="fast" for speed (lower quality)
- Consider upgrading to strategy="hi_res" with GPU support

## Next Steps

1. Install dependencies
2. Run: `python scripts/load_books_to_snowflake.py`
3. Test semantic search with queries from `docs/queries.md`
4. Compare results to previous pdfplumber approach

## References

- Unstructured docs: https://docs.unstructured.io/
- Chunking guide: https://unstructured.io/blog/chunking-for-rag-best-practices
- API reference: https://docs.unstructured.io/api-reference/api-services/chunking
