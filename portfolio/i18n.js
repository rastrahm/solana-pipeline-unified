const I18N = {
  es: {
    'html.lang': 'es',
    'meta.title': 'solana-pipeline-unified — Rolando Strahm',
    'meta.description':
      'Orquestador de aprendizaje: une mini-solana-turbine (red/FEC) con nvme-state-db (persistencia). Rust · colas acotadas · Engine KV · fases 0–8.',

    'nav.overview': 'Proyecto',
    'nav.pillars': 'Pilares',
    'nav.goals': 'Logros',
    'nav.decisions': 'Decisiones',
    'nav.benches': 'Benches',
    'nav.process': 'Proceso',
    'nav.pipeline': 'Flujo',
    'nav.repos': 'Repos',
    'nav.close': '← Cerrar',

    'hero.tag': '// MÓDULO SOLANA · PORTFOLIO SYSTEMS',
    'hero.title': 'SOLANA-PIPELINE<br>UNIFIED',
    'hero.role': 'Rust · Turbine → Bridge → NVMe Engine',
    'hero.sub':
      'Orquestador de aprendizaje que cablea red/FEC (mini-solana-turbine) con persistencia KV (nvme-state-db): colas acotadas, backpressure tipado y ciclo de vida medible — sin reimplementar el disco ni el shred.',
    'hero.cta1': 'Ver en GitHub',
    'hero.cta2': 'Ver en GitLab',

    'ov.eyebrow': '// 01 — CONTEXTO',
    'ov.title': 'Por qué este proyecto',
    'ov.lead':
      'Tras construir Turbine y el motor NVMe por separado, faltaba la pieza del medio: el <strong>pegamento</strong> de un validador — ciclo de vida, colas y fronteras de responsabilidad. Este crate es ese laboratorio: <strong>orquestación honesta</strong>, no un Agave en miniatura ni un tercer motor de I/O.',

    'pi.eyebrow': '// 02 — TRES PILARES',
    'pi.title': 'Qué entrega el orquestador',
    'p1.num': '// PILAR_01',
    'p1.title': 'Solo orquestación',
    'p1.desc':
      'Arranca y apaga Engine + Turbine. Llama APIs públicas; no abre WAL, no parsea shreds, no habla de O_DIRECT.',
    'p1.l1': 'start / shutdown / flush',
    'p1.l2': 'Path deps a hermanos',
    'p1.l3': 'Roles fijos en .cursorrules',
    'p2.num': '// PILAR_02',
    'p2.title': 'Bridge + backpressure',
    'p2.desc':
      'Cola acotada crossbeam: try_send tipado. Saturación → BridgeSaturated / PipelineStall, sin bloquear al productor.',
    'p2.l1': 'Hilo pipeline-bridge',
    'p2.l2': 'Claves learn/v1/',
    'p2.l3': 'Flush ante cola llena',
    'p3.num': '// PILAR_03',
    'p3.title': 'Ingest → Engine',
    'p3.desc':
      'Feature simd: Pipeline::ingest_bytes → shards en cola → Engine::put. Demo CLI y recovery al reabrir.',
    'p3.l1': 'IngestOutcome medible',
    'p3.l2': 'Recovery post-shutdown',
    'p3.l3': 'Bench e2e Criterion',

    'go.eyebrow': '// 03 — BÚSQUEDA Y LOGRO',
    'go.title': 'Qué se buscó y qué se logró',
    'go.lead':
      'El objetivo no era un validador: era cerrar el cableado red→FEC→cola→disco con fronteras claras y números honestos de laboratorio.',
    'go.sought': 'SE BUSCÓ',
    'go.achieved': 'SE LOGRÓ',
    'go.s1t': 'Pegar dos crates reales',
    'go.s1d':
      'Entender cómo un TVU-like entrega estado a un almacén tipo AccountsDB sin mezclar responsabilidades ni duplicar lógica.',
    'go.a1t': 'Pipeline e2e cerrado',
    'go.a1d':
      'Fases 0–8: stubs → deps → lifecycle → bridge → ingest → flush/recovery → CLI demo → benches Criterion.',
    'go.s2t': 'Backpressure explícito',
    'go.s2d':
      'Colas acotadas, errores tipados y política de flush documentada — no “esperar forever” en el hot path del productor.',
    'go.a2t': 'Lab medible y honesto',
    'go.a2d':
      'Claves learn/v1/, demo --demo, recovery verificable y benches que admiten que el WAL O_SYNC domina submit_durable.',

    'dec.eyebrow': '// 04 — DECISIONES TÉCNICAS',
    'dec.title': 'Motivo detrás de cada choice',
    'dec.lead':
      'Cada restricción fuerza el patrón correcto: este crate <strong>orquesta</strong>; Turbine hace red/FEC; nvme hace disco. Si una fase pide WAL o parseo aquí, queda fuera.',
    'dec.th1': 'Decisión',
    'dec.th2': 'Motivo / tradeoff',
    'dec.r1a': 'Solo APIs públicas de hermanos',
    'dec.r1b': 'No duplicar FEC, arena, WAL ni SST; el aprendizaje es el cableado, no un tercer motor.',
    'dec.r2a': 'Persistencia solo vía Engine',
    'dec.r2b': 'put/get/delete/flush; el orquestador nunca abre archivos de estado ni menciona O_DIRECT.',
    'dec.r3a': 'Cola acotada + try_send',
    'dec.r3b': 'Backpressure tipado (BridgeSaturated / PipelineStall) en vez de bloquear workers de red indefinidamente.',
    'dec.r4a': 'thiserror en lib; anyhow solo en main',
    'dec.r4b': 'Errores de pipeline tipados y zero-cost; bootstrap del binario con contexto flexible.',
    'dec.r5a': 'Features uring/simd reenviadas',
    'dec.r5b': 'Mismo contrato que turbine: CI sin io_uring usa --features simd; default = ambos.',
    'dec.r6a': 'Claves learn/v1/…',
    'dec.r6b': 'Convención educativa explícita: no fingir encoding de AccountsDB ni ledger real.',
    'dec.r7a': 'Analogía TVU / AccountsDB, no Blockstore',
    'dec.r7b': 'Alcance acotado: estado KV tras FEC; ledger de shreds y replay quedan fuera.',
    'dec.r8a': 'Fases con autorización explícita',
    'dec.r8b': 'TDD por gate; evita mezclar lifecycle, bridge e ingest en un solo PR opaco.',
    'dec.r9a': 'Sin unwrap/expect en src/',
    'dec.r9b': 'Producción tipada; panics reservados a tests y benches.',

    'be.eyebrow': '// 05 — BENCHMARKS',
    'be.title': 'Medir el cableado, no fingir mainnet',
    'be.lead':
      'Fase 8: Criterion e2e. El <strong>WAL O_SYNC</strong> de nvme domina <code>submit_durable</code>; el ingest sin open/close es órdenes de magnitud más rápido. Host: Intel Core Ultra 9 275HX · Linux 6.18.7 · NVMe ext4.',
    'be.s1': 'submit_durable / op',
    'be.s2': 'ops durables',
    'be.s3': 'ingest data+code',
    'be.s4': 'par + start/stop',
    'be.th1': 'Bench',
    'be.th2': 'Qué mide',
    'be.r1': 'Encolar + esperar get; WAL síncrono domina (~99 elem/s)',
    'be.r2': 'mean ~9.8 ms · p50 ~9.8 · p99 ~10.3 · ~102 ops/s',
    'be.r3': 'Criterion incluye start/shutdown del Orchestrator por iteración',
    'be.r4': 'Solo ingest (sin open/close): mean ~232 µs · p50 ~269 µs',

    'pr.eyebrow': '// 06 — PROCESO',
    'pr.title': 'Fases 0–8 cerradas',
    'pr.lead':
      'Desarrollo por gates autorizados: plan, stubs, deps path, lifecycle, bridge, ingest, flush/recovery, CLI y benches.',
    'ph.01': 'Plan + crate stubs',
    'ph.23': 'Deps path + Orchestrator',
    'ph.45': 'Bridge + ingest→Engine',
    'ph.67': 'Flush/recovery + CLI demo',
    'ph.8x': 'Bench e2e Criterion',
    'st.1': 'Fases',
    'st.2': 'Crates hermanos',
    'st.3': 'Disco en este crate',
    'st.4': 'Features (uring/simd)',
    'term.label': 'rolando@strahm:~/solana-pipeline-unified',
    'term.1': 'cargo test',
    'term.2': '[PASS] suite · phases green',
    'term.3': 'cargo run --features simd -- --demo',
    'term.4': 'ingest → bridge → Engine · shards OK',
    'term.5': 'echo status',
    'term.6': 'PHASES_0_8_CLOSED · LEARNING_DONE',

    'pl.eyebrow': '// 07 — FLUJO DEL DATO',
    'pl.title': 'De shred a Engine::put',
    'pl.lead':
      'Bytes sintéticos (o UDP vía turbine) entran al Pipeline, se reconstruyen con FEC, cruzan la cola del bridge y persisten en nvme — el orquestador solo mueve y gobierna.',
    'c1.t': 'Ingest',
    'c1.d': 'Pipeline::ingest_bytes (simd): parse + FEC.',
    'c2.t': 'Outcome',
    'c2.d': 'Reconstrucción + plan de forward (sin send en demo).',
    'c3.t': 'Queue',
    'c3.d': 'try_send a cola acotada; stall si satura.',
    'c4.t': 'Put',
    'c4.d': 'Hilo bridge → Engine::put (learn/v1/shard/N).',
    'c5.t': 'Flush',
    'c5.d': 'schedule_flush / recovery al reabrir data-dir.',

    're.eyebrow': '// 08 — CÓDIGO ABIERTO',
    're.title': 'Repositorios',
    're.lead':
      'El mismo código está en GitHub y GitLab: fuente, FASES.md, tests de integración, CLI demo y benches e2e.',
    're.cta': 'Contactar',
    're.linkedin': 'LinkedIn',

    'ft.left': 'ROLANDO STRAHM — solana-pipeline-unified · Portfolio',
    'ft.right': 'RUST · ORCHESTRATION · BRIDGE · ALL_SYSTEMS_OPERATIONAL',
  },

  en: {
    'html.lang': 'en',
    'meta.title': 'solana-pipeline-unified — Rolando Strahm',
    'meta.description':
      'Learning orchestrator: wires mini-solana-turbine (net/FEC) to nvme-state-db (persistence). Rust · bounded queues · Engine KV · phases 0–8.',

    'nav.overview': 'Project',
    'nav.pillars': 'Pillars',
    'nav.goals': 'Outcomes',
    'nav.decisions': 'Decisions',
    'nav.benches': 'Benches',
    'nav.process': 'Process',
    'nav.pipeline': 'Flow',
    'nav.repos': 'Repos',
    'nav.close': '← Close',

    'hero.tag': '// SOLANA MODULE · SYSTEMS PORTFOLIO',
    'hero.title': 'SOLANA-PIPELINE<br>UNIFIED',
    'hero.role': 'Rust · Turbine → Bridge → NVMe Engine',
    'hero.sub':
      'Learning orchestrator that wires net/FEC (mini-solana-turbine) to KV persistence (nvme-state-db): bounded queues, typed backpressure, and a measurable lifecycle — without reimplementing disk or shreds.',
    'hero.cta1': 'View on GitHub',
    'hero.cta2': 'View on GitLab',

    'ov.eyebrow': '// 01 — CONTEXT',
    'ov.title': 'Why this project',
    'ov.lead':
      'After building Turbine and the NVMe engine separately, the missing piece was the <strong>glue</strong> of a validator — lifecycle, queues, and responsibility boundaries. This crate is that lab: <strong>honest orchestration</strong>, not a mini-Agave or a third I/O engine.',

    'pi.eyebrow': '// 02 — THREE PILLARS',
    'pi.title': 'What the orchestrator delivers',
    'p1.num': '// PILLAR_01',
    'p1.title': 'Orchestration only',
    'p1.desc':
      'Starts and stops Engine + Turbine. Calls public APIs; never opens the WAL, parses shreds, or talks about O_DIRECT.',
    'p1.l1': 'start / shutdown / flush',
    'p1.l2': 'Path deps to siblings',
    'p1.l3': 'Fixed roles in .cursorrules',
    'p2.num': '// PILLAR_02',
    'p2.title': 'Bridge + backpressure',
    'p2.desc':
      'Bounded crossbeam queue: typed try_send. Saturation → BridgeSaturated / PipelineStall, without blocking the producer.',
    'p2.l1': 'pipeline-bridge thread',
    'p2.l2': 'learn/v1/ keys',
    'p2.l3': 'Flush when the queue fills',
    'p3.num': '// PILLAR_03',
    'p3.title': 'Ingest → Engine',
    'p3.desc':
      'simd feature: Pipeline::ingest_bytes → shards on the queue → Engine::put. CLI demo and recovery on reopen.',
    'p3.l1': 'Measurable IngestOutcome',
    'p3.l2': 'Post-shutdown recovery',
    'p3.l3': 'Criterion e2e bench',

    'go.eyebrow': '// 03 — INTENT AND OUTCOME',
    'go.title': 'What was sought and achieved',
    'go.lead':
      'The goal was never a validator: it was to close the net→FEC→queue→disk wiring with clear boundaries and honest lab numbers.',
    'go.sought': 'SOUGHT',
    'go.achieved': 'ACHIEVED',
    'go.s1t': 'Wire two real crates',
    'go.s1d':
      'Understand how a TVU-like path delivers state into an AccountsDB-style store without mixing duties or duplicating logic.',
    'go.a1t': 'Closed e2e pipeline',
    'go.a1d':
      'Phases 0–8: stubs → deps → lifecycle → bridge → ingest → flush/recovery → CLI demo → Criterion benches.',
    'go.s2t': 'Explicit backpressure',
    'go.s2d':
      'Bounded queues, typed errors, and a documented flush policy — no “wait forever” on the producer hot path.',
    'go.a2t': 'Honest, measurable lab',
    'go.a2d':
      'learn/v1/ keys, --demo CLI, verifiable recovery, and benches that admit WAL O_SYNC dominates submit_durable.',

    'dec.eyebrow': '// 04 — TECHNICAL DECISIONS',
    'dec.title': 'The why behind each choice',
    'dec.lead':
      'Every constraint forces the right pattern: this crate <strong>orchestrates</strong>; Turbine owns net/FEC; nvme owns disk. If a phase asks for WAL or parsing here, it stays out.',
    'dec.th1': 'Decision',
    'dec.th2': 'Rationale / tradeoff',
    'dec.r1a': 'Sibling public APIs only',
    'dec.r1b': 'No duplicated FEC, arena, WAL, or SST; the lesson is the wiring, not a third engine.',
    'dec.r2a': 'Persistence only via Engine',
    'dec.r2b': 'put/get/delete/flush; the orchestrator never opens state files or mentions O_DIRECT.',
    'dec.r3a': 'Bounded queue + try_send',
    'dec.r3b': 'Typed backpressure (BridgeSaturated / PipelineStall) instead of blocking network workers forever.',
    'dec.r4a': 'thiserror in lib; anyhow only in main',
    'dec.r4b': 'Typed, zero-cost pipeline errors; flexible bootstrap context in the binary.',
    'dec.r5a': 'Forwarded uring/simd features',
    'dec.r5b': 'Same contract as turbine: CI without io_uring uses --features simd; default = both.',
    'dec.r6a': 'learn/v1/… keys',
    'dec.r6b': 'Explicit educational convention: no fake AccountsDB encoding or real ledger.',
    'dec.r7a': 'TVU / AccountsDB analogy, not Blockstore',
    'dec.r7b': 'Bounded scope: KV state after FEC; shred ledger and replay stay out.',
    'dec.r8a': 'Explicit phase authorization',
    'dec.r8b': 'TDD per gate; avoids mixing lifecycle, bridge, and ingest into one opaque PR.',
    'dec.r9a': 'No unwrap/expect in src/',
    'dec.r9b': 'Typed production path; panics reserved for tests and benches.',

    'be.eyebrow': '// 05 — BENCHMARKS',
    'be.title': 'Measure the wiring, don’t fake mainnet',
    'be.lead':
      'Phase 8: Criterion e2e. nvme’s <strong>WAL O_SYNC</strong> dominates <code>submit_durable</code>; ingest without open/close is orders of magnitude faster. Host: Intel Core Ultra 9 275HX · Linux 6.18.7 · NVMe ext4.',
    'be.s1': 'submit_durable / op',
    'be.s2': 'durable ops',
    'be.s3': 'ingest data+code',
    'be.s4': 'pair + start/stop',
    'be.th1': 'Bench',
    'be.th2': 'What it measures',
    'be.r1': 'Enqueue + wait for get; sync WAL dominates (~99 elems/s)',
    'be.r2': 'mean ~9.8 ms · p50 ~9.8 · p99 ~10.3 · ~102 ops/s',
    'be.r3': 'Criterion includes Orchestrator start/shutdown per iteration',
    'be.r4': 'Ingest only (no open/close): mean ~232 µs · p50 ~269 µs',

    'pr.eyebrow': '// 06 — PROCESS',
    'pr.title': 'Phases 0–8 closed',
    'pr.lead':
      'Gated, authorized delivery: plan, stubs, path deps, lifecycle, bridge, ingest, flush/recovery, CLI, and benches.',
    'ph.01': 'Plan + crate stubs',
    'ph.23': 'Path deps + Orchestrator',
    'ph.45': 'Bridge + ingest→Engine',
    'ph.67': 'Flush/recovery + CLI demo',
    'ph.8x': 'Criterion e2e bench',
    'st.1': 'Phases',
    'st.2': 'Sibling crates',
    'st.3': 'Disk in this crate',
    'st.4': 'Features (uring/simd)',
    'term.label': 'rolando@strahm:~/solana-pipeline-unified',
    'term.1': 'cargo test',
    'term.2': '[PASS] suite · phases green',
    'term.3': 'cargo run --features simd -- --demo',
    'term.4': 'ingest → bridge → Engine · shards OK',
    'term.5': 'echo status',
    'term.6': 'PHASES_0_8_CLOSED · LEARNING_DONE',

    'pl.eyebrow': '// 07 — DATA FLOW',
    'pl.title': 'From shred to Engine::put',
    'pl.lead':
      'Synthetic bytes (or UDP via turbine) enter the Pipeline, reconstruct via FEC, cross the bridge queue, and land in nvme — the orchestrator only moves and governs.',
    'c1.t': 'Ingest',
    'c1.d': 'Pipeline::ingest_bytes (simd): parse + FEC.',
    'c2.t': 'Outcome',
    'c2.d': 'Reconstruction + forward plan (no send in the demo).',
    'c3.t': 'Queue',
    'c3.d': 'try_send on a bounded queue; stall if full.',
    'c4.t': 'Put',
    'c4.d': 'Bridge thread → Engine::put (learn/v1/shard/N).',
    'c5.t': 'Flush',
    'c5.d': 'schedule_flush / recovery on data-dir reopen.',

    're.eyebrow': '// 08 — OPEN SOURCE',
    're.title': 'Repositories',
    're.lead':
      'The same codebase lives on GitHub and GitLab: source, FASES.md, integration tests, CLI demo, and e2e benches.',
    're.cta': 'Contact',
    're.linkedin': 'LinkedIn',

    'ft.left': 'ROLANDO STRAHM — solana-pipeline-unified · Portfolio',
    'ft.right': 'RUST · ORCHESTRATION · BRIDGE · ALL_SYSTEMS_OPERATIONAL',
  },
};

function setLanguage(lang) {
  const dict = I18N[lang] || I18N.es;
  document.documentElement.lang = dict['html.lang'];
  document.title = dict['meta.title'];

  const metaDesc = document.querySelector('meta[name="description"]');
  if (metaDesc && dict['meta.description']) {
    metaDesc.setAttribute('content', dict['meta.description']);
  }

  document.querySelectorAll('[data-i18n]').forEach((el) => {
    const key = el.getAttribute('data-i18n');
    const val = dict[key];
    if (val == null) return;
    if (el.hasAttribute('data-i18n-html')) el.innerHTML = val;
    else el.textContent = val;
  });

  document.querySelectorAll('.lang-btn').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.lang === lang);
  });

  localStorage.setItem('pipeline-unified-portfolio-lang', lang);

  const url = new URL(window.location.href);
  url.searchParams.set('lang', lang);
  history.replaceState(null, '', url);
}

function initI18n() {
  const params = new URLSearchParams(window.location.search);
  const fromQuery = params.get('lang');
  const saved = localStorage.getItem('pipeline-unified-portfolio-lang');
  const preferred =
    (fromQuery === 'en' || fromQuery === 'es' ? fromQuery : null) ||
    saved ||
    (navigator.language?.startsWith('en') ? 'en' : 'es');

  setLanguage(preferred);

  document.querySelectorAll('.lang-btn').forEach((btn) => {
    btn.addEventListener('click', () => setLanguage(btn.dataset.lang));
  });
}

document.addEventListener('DOMContentLoaded', initI18n);
