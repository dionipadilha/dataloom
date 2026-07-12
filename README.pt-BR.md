# 🧵 DataLoom

> Um motor de orquestração multi-thread leve, eficiente e seguro para Python.

🇺🇸 [English version](README.md)

[![CI](https://github.com/dionipadilha/dataloom/actions/workflows/ci.yml/badge.svg)](https://github.com/dionipadilha/dataloom/actions/workflows/ci.yml) [![PyPI](https://img.shields.io/pypi/v/dataloom-engine)](https://pypi.org/project/dataloom-engine/) [![Python](https://img.shields.io/pypi/pyversions/dataloom-engine)](https://pypi.org/project/dataloom-engine/) ![License](https://img.shields.io/badge/license-MIT-green)

<img width="2752" height="1536" alt="DataLoom: Um motor de orquestração multi-thread leve, eficiente e seguro para Python." src="https://github.com/user-attachments/assets/c1f3fc98-5eaf-4add-a38c-2717ae699cd5" />

O **DataLoom** é uma biblioteca projetada para processar fluxos de dados utilizando o padrão **Produtor-Consumidor** com múltiplas threads (Weavers). Ele abstrai a complexidade de filas (`Queues`), sincronização (`Locks`) e gerenciamento de ciclo de vida, permitindo que você foque apenas na lógica de transformação dos dados.

## Por que usar o DataLoom?
⚡ Performance: Processamento paralelo real para tarefas I/O bound.
🧩 Simplicidade: API intuitiva inspirada na metáfora de tecelagem.
🛡️ Segurança: Thread-safety garantido por design em todo o pipeline.
📦 Leve: Dependências mínimas, pronto para rodar em qualquer ambiente Python.

### Por que não usar só `ThreadPoolExecutor`?

Pergunta justa — a stdlib resolve o *paralelismo*, mas não o *pipeline*. O
`concurrent.futures` te dá um pool de threads; tudo ao redor fica por sua conta:

| Você precisa de...                                  | Com a stdlib             | Com o DataLoom                    |
| --------------------------------------------------- | ------------------------ | --------------------------------- |
| Fluxo contínuo produtor-consumidor                   | `Queue` + loops manuais  | `Loom` + `Source`                 |
| Backpressure (produtor mais rápido que consumidores) | `Queue(maxsize=...)` manual | Padrão, configurável            |
| Shutdown limpo (drenar fila, fechar recursos)        | Sentinelas e joins manuais | `stop()` / `with Loom(...)`     |
| Worker que sobrevive a erros e os reporta            | try/except em cada worker | `hooks.on_error` centralizado    |
| Métricas por item processado                         | Instrumentação manual    | `hooks.on_batch_processed`        |
| Escrita concorrente segura em arquivo                | Lock manual              | Sinks prontos (JSON, CSV, callback) |

Se o seu caso é "aplicar uma função a uma lista e coletar os resultados",
use `ThreadPoolExecutor.map` — é a ferramenta certa. O DataLoom é para
**fluxos contínuos ou longos** onde ciclo de vida, resiliência e
observabilidade importam. E, como toda solução baseada em threads no
CPython, o ganho de paralelismo vale para cargas **I/O bound**; para
CPU bound, prefira multiprocessing.

## ✨ Características

- **API Coesa:** Conceitos alinhados (`Loom`, `Weaver`, `Sink`).
- **Concorrente:** Gerencia automaticamente múltiplos _Weavers_ (threads) em paralelo.
- **Seguro:** Sinks padrão são thread-safe e exceções são tipadas (`LoomError`).
- **Resiliente:** Erros de processamento não derrubam os Weavers e são reportados via hooks.
- **Gerenciável:** `Loom` é um context manager — `with Loom(...) as loom:` garante `stop()` automático.
- **Backpressure:** Fila de tarefas limitada por padrão (`queue_maxsize`), evitando crescimento de memória sem controle.
- **Observável:** Hooks de ciclo de vida e métricas por lote (`on_batch_processed` com resultado e duração), além de Logs integrados.

## 📦 Instalação

```bash
pip install dataloom-engine
```

> ⚠️ **Atenção ao nome:** o pacote instala o módulo `dataloom_engine`
> (`import dataloom_engine`). Não confunda com o pacote `dataloom` do PyPI,
> que é um ORM de outro autor e não tem relação com este projeto.

Para desenvolver ou usar a versão mais recente do repositório:

```bash
git clone https://github.com/dionipadilha/dataloom.git
cd dataloom
pip install -e .
```

## 🚀 Uso Rápido

Aqui está um exemplo completo de como tecer um pipeline de dados:

```python
from pathlib import Path
import numpy as np
from dataloom_engine import (
    Loom,
    LoomConfig,
    LoomLogs,
    Processor,
    JsonFileSink,
    ThreadedBufferedSink
)
from dataloom_engine.sources import RandomNumPySource

# 1. Defina sua lógica de processamento (Stateless)
class MyFilterProcessor(Processor):
    def process(self, batch: np.ndarray) -> dict:
        # 'batch' é um array numpy com o tamanho definido na config
        avg = float(batch.mean())
        return {
            "processed_items": len(batch),
            "average_value": avg,
            "status": "high" if avg > 0.5 else "low"
        }

# 2. Configuração Inicial
if __name__ == "__main__":
    # Configura logs no console
    LoomLogs.setup()

    # Define parâmetros do motor
    config = LoomConfig(
        output_dir=Path("./data_out"),
        batch_size=100,      # Processa 100 itens por vez
        interval_seconds=1   # Gera um novo lote a cada 1 segundo
    )

    # 3. Fonte de Dados
    # Agora desvinculada do motor, permitindo fontes customizadas (DB, CSV, S3)
    source = RandomNumPySource(config)

    # 4. Sink Arquivado e Otimizado
    # ThreadedBufferedSink evita que o I/O bloqueie o processamento
    file_sink = JsonFileSink(config.output_dir)
    sink = ThreadedBufferedSink(file_sink)

    # 5. Inicializa o Loom (O Orquestrador)
    loom = Loom(
        config=config,
        processor=MyFilterProcessor(),
        sink=sink,
        source=source,
        num_weavers=4  # 4 Threads trabalhando em paralelo
    )

    print("🧵 DataLoom iniciado! Pressione Ctrl+C para parar.")
    try:
        # O context manager garante stop() e limpeza dos recursos,
        # mesmo em caso de exceção ou Ctrl+C
        with loom:
            loom.start()
    except KeyboardInterrupt:
        print("\n🛑 Tear parado.")
```

## 🏗️ Arquitetura

O DataLoom utiliza uma metáfora de tecelagem:

- **Loom (Tear):** A máquina principal. Gerencia a fila de tarefas e o ciclo de vida.
- **Weaver (Tecelão):** As threads trabalhadoras. Elas pegam a matéria-prima (batch), processam e entregam.
- **Processor:** A lógica de negócio. Transforma dados brutos em informação.
- **Sink:** O destino final. Onde o produto acabado é depositado (ex: `JsonFileSink`, `CsvFileSink`, ou qualquer destino via `CallbackSink`).

## 🛠️ Desenvolvimento e Testes

Para contribuir com o projeto ou rodar os testes unitários:

1.  Instale as dependências de desenvolvimento:

    ```bash
    pip install -e ".[dev]"
    ```

2.  Rode a suíte de testes (via pytest):
    ```bash
    pytest
    ```

3.  Rode o linter e o verificador de tipos:
    ```bash
    ruff check .
    mypy
    ```

## 📄 Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.

## 🗒️ Histórico de versões

As mudanças de cada versão estão documentadas no [CHANGELOG](CHANGELOG.md).
