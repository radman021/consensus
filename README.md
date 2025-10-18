# University Project – Distributed Algorithms

A university project implementing a consensus based on the **NBFT (Nested Byzantine Fault Tolerance)** algorithm.


## 🔗 References

- **Original Paper:** [NBFT: Improved Fault-Tolerant Consensus Based on PBFT Algorithm](https://drive.google.com/file/d/124d21wv1ItTmhhlMhkDjwjKzKuYg0jGR/view?usp=drive_link)  
- **Project Documentation:** [Documentation](https://drive.google.com/file/d/1M0989867eeSwTj-IeedsfDLlnqV5l63P/view?usp=drive_link)

---

## Setup and Installation

### 1. Clone the repository
```bash
git clone https://github.com/radman021/consensus
cd consensus
```

### 2. Install dependencies

```
pip install -r requirements.txt
```

### 3. Run Redis with Docker

```
docker compose up -d
```

### 4. Run algorithm

```
python src/run.py
```