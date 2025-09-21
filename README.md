# Brewery Project – Execução local com Docker

Pipeline de dados com Prefect 2, Spark e MinIO. Este guia mostra como subir tudo com Docker e rodar o fluxo localmente de forma reprodutível.

## Visão geral
- Orquestração: Prefect Server (UI em `http://localhost:4200`) e um agente Prefect
- Armazenamento de objetos: MinIO (console em `http://localhost:9001` – usuário `minio`, senha `minio123`)
- Processamento: Spark
- Banco do Prefect: Postgres (via Docker)

Estrutura principal usada:
- `docker-compose.yaml` – define os serviços
- `deployment.py` – cria/aplica o deployment do Prefect para o fluxo `workflow/flow.py:full_brewery_pipeline`

## Pré‑requisitos
- Docker Desktop (Compose v2)
- Porta 4200 livre (Prefect UI), portas 9000/9001 livres (MinIO)

Não é necessário Python instalado na máquina: os comandos serão executados dentro do container `prefect`.

## Subindo a stack
1) Build das imagens (inclui imagens custom de Spark e Prefect)
```powershell
docker compose build
```

2) Subir os serviços
```powershell
docker compose up -d
```

3) Acessos rápidos
- Prefect UI: `http://localhost:4200`
- MinIO Console: `http://localhost:9001` (user: `minio`, pass: `minio123`)

## Iniciar o agente do Prefect
O agente precisa estar rodando e ouvindo a fila `default` (já configurada no compose via `PREFECT_API_URL`).

- Interativo (anexa o terminal):
```powershell
docker exec -it prefect bash -lc "prefect agent start -q default"
```

- Em background (não bloqueia seu terminal):
```powershell
docker exec -d prefect bash -lc "prefect agent start -q default"
```

Você pode acompanhar as execuções pela UI do Prefect ou ver logs anexando ao container do agente.

## Criar e aplicar o deployment
O arquivo `deployment.py` gera um YAML em `prefect_data/full_brewery_pipeline-deployment.yaml` e tenta aplicar o deployment automaticamente.

Execute dentro do container `prefect`:
```powershell
docker exec -it prefect bash -lc "python deployment.py"
```

Se a aplicação automática falhar (ex.: API do Prefect ainda subindo), aplique manualmente o YAML:
```powershell
docker exec -it prefect bash -lc "prefect deployment apply prefect_data/full_brewery_pipeline-deployment.yaml"
```docker exec -it prefect bash -lc "python -c \"from workflow.flow import full_brewery_pipeline; full_brewery_pipeline()\""docker exec -it prefect bash -lc "python -c `\"from workflow.flow import full_brewery_pipeline; full_brewery_pipeline()`\""

Observação: o deployment já inclui um agendamento (cron) para rodar a cada hora.

## Executar o fluxo sob demanda
Liste e rode o deployment manualmente:
```powershell
docker exec -it prefect bash -lc "prefect deployment ls"
docker exec -it prefect bash -lc "prefect deployment run 'full-brewery-pipeline/full-brewery-pipeline-deployment'"
```

## Parar e limpar
- Parar os serviços:
```powershell
docker compose down
```

- Parar e remover volumes (dados do MinIO e Postgres serão apagados):
```powershell
docker compose down -v
```

## Solução de problemas
- UI do Prefect não abre: verifique se a porta 4200 está livre e o container `prefect-server` está saudável (`docker ps`).
- Agent não consome tarefas: confirme que ele está rodando e ouvindo a fila `default`.
	- Dica: rode novamente `docker exec -it prefect bash -lc "prefect agent start -q default"`.
- Erros de import do Prefect (no editor): garanta que você está executando os comandos dentro do container `prefect`, que já tem as dependências corretas (versões fixadas em `requirements.txt`).
- Após mudar `requirements.txt`: faça rebuild da imagem do `prefect` para garantir as libs atualizadas:
```powershell
docker compose build --no-cache prefect
docker compose up -d
```
- MinIO credenciais padrão: usuário `minio`, senha `minio123` (definidos no `docker-compose.yaml`).

## Estrutura do fluxo
O flow principal está em `workflow/flow.py` como `full_brewery_pipeline`. O deployment usa o entrypoint:
```
workflow/flow.py:full_brewery_pipeline
```
As transformações estão em `stages/` (bronze, silver, gold) e utilidades em `utils/`.

---

Com isso, qualquer pessoa consegue subir a stack, aplicar o deployment e executar o pipeline localmente usando Docker.
