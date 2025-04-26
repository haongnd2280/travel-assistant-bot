# The assistant retrieve policy information to answer user questions.
# Note that enforcement of these policies still must be done within the tools/APIs themselves,
# since the LLM can always ignore this.

# ---> “Don’t assume that just because the LLM retrieved the right policy text, it will obey it.
# You still need enforcement logic in your downstream tools.”

import re
from typing import TypedDict, TypeAlias, Self, ClassVar
from collections.abc import Sequence

import requests
import numpy as np
from openai import OpenAI

from langchain_core.tools import tool

from dotenv import load_dotenv
load_dotenv()


# Downloads a markdown FAQ file from a URL
# Reads its content as a plain string
faq_file_url = "https://storage.googleapis.com/benchmarks-artifacts/travel-db/swiss_faq.md"
response = requests.get(faq_file_url)
response.raise_for_status()
faq_text = response.text

# Splits the FAQ into chunks based on section headers (## ...)
# using a regex with a lookahead ((?=\n##)).
# Each section becomes a dictionary like {"page_content": "..."}
# — a common format used by LangChain for documents.
docs = [
    {"page_content": txt} for txt in re.split(r"(?=\n##)", faq_text)
]

class Document(TypedDict):
    page_content: str

Documents: TypeAlias = Sequence[Document]
Vector: TypeAlias = Sequence[float]
Vectors: TypeAlias = Sequence[Vector]


class VectorStoreRetriever:
    """A vector store (just a NumPy array) for fast similarity search,
    using OpenAI embeddings.
    """

    _embed_model: ClassVar[str] = "text-embedding-3-small"

    def __init__(
        self,
        docs: Documents,
        vectors: Vectors,
        client: OpenAI,
    ):
        """Create a vector store retriever from documents and their corresponding embedding vectors.
        """
        self._arr = np.array(vectors)
        self._docs = docs
        self._client = client

    @classmethod
    def from_docs(
        cls,
        docs: Documents,
        client: OpenAI,
    ) -> Self:
        """Create a vector store retriever from documents.
        """
        embeddings = client.embeddings.create(
            model=cls._embed_model,
            input=[doc["page_content"] for doc in docs]
        )
        vectors = [emb.embedding for emb in embeddings.data]
        return cls(docs, vectors, client)

    def query(self, query: str, top_k: int = 5) -> list[dict]:
        """Query the vector store for the top-k most similar documents to the query.
        """
        # Embed the query
        embed = self._client.embeddings.create(
            model=self._embed_model,
            input=[query]
        )
        # Computes cosine similarity scores
        # Since embeddings are normalized, dot product = cosine similarity
        # ("@" is just a matrix multiplication in python)
        scores = np.array(embed.data[0].embedding) @ self._arr.T

        # Finds indices of the top-k scores using efficient NumPy methods
        top_k_idx = np.argpartition(scores, -top_k)[-top_k:]
        top_k_idx_sorted = top_k_idx[np.argsort(-scores[top_k_idx])]

        # Returns the top-k docs along with their similarity score
        return [
            {**self._docs[idx], "similarity": scores[idx]}
            for idx in top_k_idx_sorted
        ]


retriever = VectorStoreRetriever.from_docs(docs=docs, client=OpenAI())


@tool
def lookup_policy(query: str) -> str:
    """Consult the company policies to check whether certain options are permitted.
    Use this before making any flight changes performing other 'write' events.
    """
    docs = retriever.query(query, top_k=2)
    return "\n\n".join([doc["page_content"] for doc in docs])
