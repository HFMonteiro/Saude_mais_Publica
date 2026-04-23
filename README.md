# Transparência SNS - Exploração de Modelos Semânticos

Projeto criado em `C:\Users\hugof\agentplayground\transparencia_connect`.

## O que o projeto faz

- Consome a API pública do portal
  `https://transparencia.sns.gov.pt/api/explore/v2.1`.
- Cria um proxy local para evitar problemas de CORS.
- Expõe endpoints auxiliares (`/api/analysis`, `/api/dataset/<id>`, ...).
- Apresenta no frontend:
  - Página inicial de arranque por mega-temas.
  - Lista de datasets disponíveis.
  - Rede de ligações com base em campos em comum entre datasets.
  - Painel de cruzamentos para o dataset selecionado.
  - Oportunidades de ligação por campo partilhado.
  - Tabela de registos recentes, focada nos últimos 3 anos quando há campo temporal identificável.

## Como correr

No terminal:

```bash
python server.py
```

Aceder a:

- `http://127.0.0.1:8000`

## Endpoints locais

- `GET /api/analysis?min_score=1`  
  Gera o grafo de ligações entre datasets.
- `GET /api/dataset/<dataset_id>`  
  Liga-se ao endpoint original do dataset no portal SNS.
- `GET /api/records/<dataset_id>?limit=50`  
  Consulta registos de um dataset.
- `GET /api/recent/<dataset_id>?limit=60`  
  Consulta registos recentes e identifica o campo temporal provável.
- `GET /api/health`

## Estrutura

- `server.py`  
  Backend simples em Python (`http.server`) com cache e análise semântica.
- `index.html`  
  Interface principal.
- `app.js`  
  Renderização do grafo, da árvore e das sugestões de cruzamento.
- `styles.css`  
  Estilos da interface.

## Notas

Esta implementação está pronta para funcionar com os dados expostos pelo portal.
Se quiseres, no próximo passo podemos adicionar uma visualização em 3D/diagrama mais avançada
ou um export CSV de pares de cruzamento recomendados para cada dataset.
