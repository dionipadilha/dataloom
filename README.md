# 🧵 DataLoom

> Um motor de orquestração multi-thread leve, eficiente e seguro para Python.

[![CI](https://github.com/dionipadilha/dataloom/actions/workflows/ci.yml/badge.svg)](https://github.com/dionipadilha/dataloom/actions/workflows/ci.yml) ![Python](https://img.shields.io/badge/python-3.9%2B-blue) ![License](https://img.shields.io/badge/license-MIT-green)

O **DataLoom** é uma biblioteca projetada para processar fluxos de dados utilizando o padrão **Produtor-Consumidor** com múltiplas threads (Weavers). Ele abstrai a complexidade de filas (`Queues`), sincronização (`Locks`) e gerenciamento de ciclo de vida, permitindo que você foque apenas na lógica de transformação dos dados.

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

```python
from pathlib import Path
import numpy as np
from dataloom import (
    Loom,
    LoomConfig,
    LoomLogs,
    Processor,
    JsonFileSink
)

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

    # 3. Inicializa o Loom (O Orquestrador)
    loom = Loom(
        config=config,
        processor=MyFilterProcessor(),
        sink=JsonFileSink(config.output_dir), # Salva em ./data_out/results.json
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
