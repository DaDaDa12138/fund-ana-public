(async function () {
  const status = document.getElementById('historyStatus');
  const target = document.getElementById('historyRows');
  const params = new URLSearchParams(location.search);
  const run = params.get('run_id');
  if (!run) { status.textContent = '未指定历史报告'; return; }
  const query = params.has('revision') ? `?revision=${encodeURIComponent(params.get('revision'))}` : '';
  try {
    const response = await fetch(`/api/reports/history/${encodeURIComponent(run)}${query}`);
    const value = await response.json();
    if (!response.ok) throw new Error(value.error || `查询失败（${response.status}）`);
    const label = value.semantic === 'committed' ? '已正式提交' : value.semantic === 'legacy_eligible' ? '旧版可发布快照' : '诊断记录';
    status.textContent = `${run} · ${label} · 原发布时间 ${value.generated_at || '未知'} · ${value.counts?.final_count ?? '未知'} 只基金`;
    const labels = { formal: '下载当期正式榜', tracker: '下载当期累计跟踪', forward_validation: '下载当期前瞻验证' };
    const exports = document.getElementById('historyExports');
    for (const [key, item] of Object.entries(value.views || {})) {
      const link = document.createElement('a'); link.className = 'download-link';
      link.href = item.export_url; link.textContent = labels[key] || key; exports.appendChild(link);
    }
    const rows = value.formal_rows;
    if (!Array.isArray(rows)) { target.textContent = '当期明细未记录'; return; }
    if (!rows.length) { target.textContent = '该期正式名单为空'; return; }
    const preferred = ['基金代码', '基金名称', '正式榜分组', '正式投资评分', '1年-年化收益(%)', '1年-最大回撤(%)'];
    const columns = preferred.filter(key => rows.some(row => key in row));
    if (!columns.length) columns.push(...Object.keys(rows[0]).slice(0, 7));
    const table = document.createElement('table'); table.className = 'data-table';
    const head = table.createTHead().insertRow();
    for (const key of columns) { const cell = document.createElement('th'); cell.textContent = key; head.appendChild(cell); }
    const body = table.createTBody();
    for (const row of rows) { const tr = body.insertRow(); for (const key of columns) tr.insertCell().textContent = row[key] ?? '-'; }
    target.appendChild(table);
  } catch (error) { status.textContent = `历史查询失败：${error.message}`; target.textContent = '数据未加载'; }
})();
