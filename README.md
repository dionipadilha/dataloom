# 🧵 DataLoom

> Um motor de orquestração multi-thread leve, eficiente e seguro para Python.

[![CI](https://github.com/dionipadilha/dataloom/actions/workflows/ci.yml/badge.svg)](https://github.com/dionipadilha/dataloom/actions/workflows/ci.yml) ![Python](https://img.shields.io/badge/python-3.9%2B-blue) ![License](https://img.shields.io/badge/license-MIT-green)

<img width="2752" height="1536" alt="DataLoom: Um motor de orquestração multi-thread leve, eficiente e seguro para Python." src="https://github.com/user-attachments/assets/c1f3fc98-5eaf-4add-a38c-2717ae699cd5" />

O **DataLoom** é uma biblioteca projetada para processar fluxos de dados utilizando o padrão **Produtor-Consumidor** com múltiplas threads (Weavers). Ele abstrai a complexidade de filas (`Queues`), sincronização (`Locks`) e gerenciamento de ciclo de vida, permitindo que você foque apenas na lógica de transformação dos dados.

## Por que usar o DataLoom?
⚡ Performance: Processamento paralelo real para tarefas I/O bound.
🧩 Simplicidade: API intuitiva inspirada na metáfora de tecelagem.
🛡️ Segurança: Thread-safety garantido por design em todo o pipeline.
📦 Leve: Dependências mínimas, pronto para rodar em qualquer ambiente Python.

## ✨ Características

- **API Coesa:** Conceitos alinhados (`Loom`, `Weaver`, `Sink`).
- **Concorrente:** Gerencia automaticamente múltiplos _Weavers_ (threads) em paralelo.
- **Seguro:** Sinks padrão são thread-safe e exceções são tipadas (`LoomError`).
- **Observável:** Hooks para monitoramento e Logs integrados.

## 📦 Instalação

Como este projeto está em desenvolvimento local, instale-o em modo editável:

```bash
git clone https://github.com/dionipadilha/dataloom.git
cd dataloom
pip install -e .
```

## 🚀 Uso Rápido

Aqui está um exemplo completo de como tecer um pipeline de dados:

from pathlib import Path
import numpy as np
from dataloom import (
    Loom,
    LoomConfig,
    LoomLogs,
    Processor,
    JsonFileSink,
    ThreadedBufferedSink
)
from dataloom.sources import RandomNumPySource

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

    try:
        print("🧵 DataLoom iniciado! Pressione Ctrl+C para parar.")
        loom.start()
    except KeyboardInterrupt:
        print("\n🛑 Parando o tear...")
        loom.stop()
```

## 🏗️ Arquitetura

O DataLoom utiliza uma metáfora de tecelagem:

- **Loom (Tear):** A máquina principal. Gerencia a fila de tarefas e o ciclo de vida.
- **Weaver (Tecelão):** As threads trabalhadoras. Elas pegam a matéria-prima (batch), processam e entregam.
- **Processor:** A lógica de negócio. Transforma dados brutos em informação.
- **Sink:** O destino final. Onde o produto acabado é depositado (ex: Arquivo JSON, Banco de Dados).

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

## 📄 Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.
