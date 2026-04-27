# Review top 10 melhorias

Estado: revisão aplicada em 2026-04-27, mantendo HTML/CSS/JS puro e o servidor Python atual.

1. Timeouts nos pedidos frontend.
   Aplicado: `app.js`, `analytics.js` e `crosswalk.js` usam `fetchJson` com `AbortController` para evitar requests pendurados.

2. Erros de API mais consistentes no frontend.
   Aplicado: respostas não-JSON ou HTTP != 2xx são normalizadas antes de chegar à UI.

3. Evitar race conditions nos carregamentos de cruzamentos.
   Aplicado: `crosswalk.js` ignora respostas antigas quando há reloads rápidos.

4. Tabs de Analytics mais previsíveis.
   Aplicado: tabs atualizam `?tab=...`, usam `aria-selected`, `role=tab` e evitam render quando se clica na tab já ativa.

5. Acessibilidade e conforto visual.
   Aplicado: foco visível também para links; suporte a `prefers-reduced-motion` mantido.

6. Robustez de Retry-After.
   Aplicado: `server.py` interpreta `Retry-After` numérico e em formato HTTP-date.

7. Coalescing sem crescimento indefinido.
   Aplicado: limpeza defensiva do mapa de locks de fetch quando há muitas chaves antigas.

8. Limpeza de artefactos locais.
   Aplicado: `scratch/` entra no `.gitignore`; nada foi apagado.

9. Manter cache leve.
   Recomendação: manter os limites atuais em memória e evitar cache persistente até haver necessidade real de refresh offline.

10. Testes de smoke recorrentes.
    Recomendação: consolidar um script de smoke local que corra `py_compile`, `node --check` e checks HTTP básicos antes de cada push.
