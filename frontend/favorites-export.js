/* Export the rendered query snapshot; do not re-query fund APIs. */
window.FavoriteExcelExport = (() => {
  function visibleCell(source, highlight = false) {
    const root = document.createElement('div');
    if (source instanceof Element) root.append(source.cloneNode(true));
    else root.innerHTML = String(source ?? '');
    root.querySelectorAll('[role="tooltip"], .score-tooltip-card, .table-inline-link, .sort-indicator, [aria-hidden="true"]').forEach((node) => node.remove());
    const target = root.querySelector('.score-tooltip-trigger') || root;
    function readText(node) {
      if (node.nodeType === Node.TEXT_NODE) return node.textContent.replace(/\s+/gu, ' ');
      if (node.nodeType !== Node.ELEMENT_NODE) return '';
      if (node.tagName === 'BR') return '\n';
      const content = Array.from(node.childNodes, readText).join('');
      return ['DIV', 'P', 'SMALL'].includes(node.tagName) ? `\n${content}\n` : content;
    }
    const text = readText(target).split('\n').map((line) => line.replace(/\s+/gu, ' ').trim()).filter(Boolean).join('\n');
    const missing = !text || (root.querySelector('.favorite-detail-value-empty') && ['-', '暂无数据'].includes(text));
    const hasClass = (name) => target.classList.contains(name) || Boolean(target.querySelector(`.${name}`));
    const result = { text: missing ? null : text, tone: hasClass('num-pos') ? 'positive' : hasClass('num-neg') ? 'negative' : 'default', highlight };
    const missingReason = root.querySelector('[data-missing-reason]')?.dataset.missingReason;
    if (missing && missingReason) result.comment = missingReason;
    return result;
  }

  function buildSnapshot(tables, metadata, labelResolver) {
    return {
      schema_version: 1,
      metadata,
      tables: tables.map(({ name, result, options, container }) => {
        const columns = result.columns.filter((key) => key !== (options.detailColumnLabel || '详情'));
        return {
          name,
          columns: columns.map((key) => ({ key, label: labelResolver(key) })),
          rows: result.sortedRows.map((row) => {
            const code = String(row['基金代码'] ?? '').trim();
            const rowNode = container.querySelector(`tr[data-fund-code="${CSS.escape(code)}"]`)
              || container.querySelector(`article[data-fund-code="${CSS.escape(code)}"]`);
            return {
              source: row['对比来源'] === '共同命中' ? 'shared' : row['对比来源'] === '优质推荐池' ? 'recommendation' : 'user',
              cells: columns.map((key) => {
                const cellNode = rowNode?.querySelector(`td[data-column="${CSS.escape(key)}"]`)
                  || rowNode?.querySelector(`.data-card-item[data-column="${CSS.escape(key)}"] > span`);
                const markup = cellNode || resolveTableCellRenderer(options.cellRenderers, key, row[key], row);
                const highlight = String(options.cellClassName?.(key, row[key], row) || '').includes('favorite-positive-return-highlight');
                const display = visibleCell(markup, highlight);
                const returnSources = {
                  '近1周(%)': '__favorite_return_1w_source',
                  '近1月(%)': '__favorite_return_1m_source',
                  '近3月(%)': '__favorite_return_3m_source',
                  '近6月(%)': '__favorite_return_6m_source',
                };
                if (returnSources[key] && display.text !== null) {
                  const live = ['live_api', 'live_fallback'].includes(row[returnSources[key]]);
                  const date = live ? row.__favorite_live_return_date : (row.__favorite_snapshot_return_date || row['区间收益基准日']);
                  display.comment = `${live ? '实时API收益' : '分析快照收益'}；${date ? `收益截至 ${date}` : '收益基准日未记录'}`;
                }
                return display;
              }),
            };
          }),
        };
      }),
    };
  }

  async function download(snapshot) {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 60000);
    try {
      const response = await fetch('/api/favorites/export', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(snapshot), signal: controller.signal,
      });
      if (!response.ok) {
        const error = await response.json().catch(() => ({}));
        throw new Error(error.error || `下载失败（${response.status}）`);
      }
      const blob = await response.blob();
      const disposition = response.headers.get('Content-Disposition') || '';
      const encodedName = disposition.match(/filename\*=UTF-8''([^;]+)/iu)?.[1];
      const filename = encodedName ? decodeURIComponent(encodedName) : '收藏基金.xlsx';
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.append(link);
      link.click();
      link.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
      return filename;
    } finally {
      window.clearTimeout(timeout);
    }
  }
  return { visibleCell, buildSnapshot, download };
})();
