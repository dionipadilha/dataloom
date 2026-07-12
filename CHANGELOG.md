# Changelog

Todas as mudanças notáveis deste projeto são documentadas neste arquivo.

O formato segue o [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e o projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [0.2.0] - 2026-07-12

### Corrigido

- Weavers não morrem mais silenciosamente quando o `Processor` ou o `Sink`
  lançam exceção: o erro é convertido em `WeaverError`, logado e reportado
  via `hooks.on_error`, e a thread continua processando os lotes seguintes.
- O encerramento (`Loom.stop()`) não usa mais `task_queue.join()`, que podia
  travar para sempre se um Weaver encerrasse com itens ainda na fila (por
  exemplo, em um `KeyboardInterrupt`). Cada Weaver agora drena a fila até
  consumir seu sentinela de parada.
- `stop()` é idempotente e não sobrescreve mais o estado `FAILED` com
  `COMPLETED` após um erro no source.
- `ThreadedBufferedSink` não perde mais itens quando `close()` é chamado
  imediatamente após `send()` (corrida entre sinalização e drenagem do
  buffer). Falha no sink alvo não derruba mais a thread de escrita.

### Adicionado

- `Loom` funciona como context manager: `with Loom(...) as loom:` garante
  `stop()` e limpeza de recursos mesmo em exceções ou `Ctrl+C`.
- Backpressure: a fila de tarefas é limitada por padrão
  (`num_weavers * 4`), configurável via `LoomConfig.queue_maxsize`
  (`0` = ilimitada).
- Validação de parâmetros no `LoomConfig` (`__post_init__`), lançando
  `ConfigurationError`; `output_dir` aceita `str` e é convertido para
  `Path`; `interval_seconds` aceita valores fracionários.
- Métricas por lote: `LoomHooks.on_batch_processed(result, duration_seconds)`,
  invocado após cada lote entregue com sucesso ao Sink.
- Novos sinks: `CsvFileSink` (escrita CSV thread-safe) e `CallbackSink`
  (delega resultados a um callable do usuário, com `on_close` opcional).
- `send()` após `close()` no `ThreadedBufferedSink` lança `LoomError` em
  vez de descartar dados em silêncio.
- `WeaverError` e `ConfigurationError` exportados na API pública.
- Marcador `py.typed`: consumidores com mypy/pyright enxergam as anotações
  de tipo da biblioteca.

### Alterado

- A assinatura interna de `Weaver` mudou (recebe callbacks `on_error` e
  `on_batch_processed` em vez de `stop_event`). O módulo é interno e não
  faz parte da API pública.
- CI atualizado: `actions/checkout@v4`, `setup-python@v5`, instalação via
  extras `[dev]`, cobertura com `pytest --cov` e Python 3.13 na matriz.

## [0.1.0] - 2026-07-05

### Adicionado

- Versão inicial: orquestração produtor-consumidor com múltiplas threads
  (`Loom`, `Weaver`), contratos de `Processor`, `Sink` e `Source`,
  `JsonFileSink`, `ThreadedBufferedSink`, hooks de ciclo de vida
  (`LoomHooks`), logging (`LoomLogs`) e exceções tipadas (`LoomError`).

[0.2.0]: https://github.com/dionipadilha/dataloom/releases/tag/v0.2.0
[0.1.0]: https://github.com/dionipadilha/dataloom/releases/tag/v0.1.0
