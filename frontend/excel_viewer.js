function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function syncStickyTableOffset() {
  const header = document.querySelector('.site-header');
  if (!header) return;
  const height = Math.ceil(header.getBoundingClientRect().height || 0);
  document.documentElement.style.setProperty('--sticky-table-top', `${Math.max(height, 0)}px`);
}

function syncTableWrapStickyOffsets(root = document) {
  const header = document.querySelector('.site-header');
  if (!header) return;
  const headerHeight = Math.ceil(header.getBoundingClientRect().height || 0);
  root.querySelectorAll('.table-wrap').forEach((wrap) => {
    const rect = wrap.getBoundingClientRect();
    const localTop = Math.max(headerHeight - rect.top, 0);
    wrap.style.setProperty('--sticky-table-local-top', `${Math.max(localTop, 0)}px`);
  });
}

function syncStickyTableLayout(root = document) {
  syncStickyTableOffset();
  syncTableWrapStickyOffsets(root);
}

window.addEventListener('resize', () => syncStickyTableLayout());
window.addEventListener('load', () => syncStickyTableLayout());
window.addEventListener('scroll', () => syncTableWrapStickyOffsets(), { passive: true });
syncStickyTableLayout();

function highlightText(value, keyword) {
  const text = String(value ?? '');
  if (!keyword) return escapeHtml(text);
  const lower = text.toLowerCase();
  const needle = keyword.toLowerCase();
  if (!lower.includes(needle)) return escapeHtml(text);

  let result = '';
  let cursor = 0;
  while (cursor < text.length) {
    const index = lower.indexOf(needle, cursor);
    if (index === -1) {
      result += escapeHtml(text.slice(cursor));
      break;
    }
    result += escapeHtml(text.slice(cursor, index));
    result += `<mark>${escapeHtml(text.slice(index, index + needle.length))}</mark>`;
    cursor = index + needle.length;
  }
  return result;
}

function getPathParam() {
  const params = new URLSearchParams(window.location.search);
  return (params.get('path') || '').trim();
}

function getSheetParam() {
  const params = new URLSearchParams(window.location.search);
  return (params.get('sheet') || '').trim();
}

function normalizeProjectPath(path) {
  return String(path || '')
    .replace(window.location.origin, '')
    .replace(/^\//, '');
}

function fileUrl(path, download = false) {
  const relative = normalizeProjectPath(path);
  return `/api/files/${relative}${download ? '?download=1' : ''}`;
}

function renderViewerFailureState({
  title = '加载失败',
  message = '请稍后重试',
  actionLabel = '',
  onAction = null,
  linkLabel = '',
  linkHref = '',
} = {}) {
  viewerTitle.textContent = title;
  viewerStatus.textContent = '加载失败';
  viewerTableWrap.innerHTML = `
    <div class="excel-preview-empty">
      <strong>${escapeHtml(title)}</strong>
      <span>${escapeHtml(message)}</span>
      <div class="retryable-empty-state-actions">
        ${actionLabel ? `<button type="button" class="secondary" data-viewer-retry="1">${escapeHtml(actionLabel)}</button>` : ''}
        ${linkLabel && linkHref ? `<a class="preview-link" href="${escapeHtml(linkHref)}">${escapeHtml(linkLabel)}</a>` : ''}
      </div>
    </div>
  `;
  if (actionLabel && typeof onAction === 'function') {
    viewerTableWrap.querySelector('[data-viewer-retry="1"]')?.addEventListener('click', async () => {
      const button = viewerTableWrap.querySelector('[data-viewer-retry="1"]');
      if (!button || button.disabled) return;
      button.disabled = true;
      button.textContent = '重试中';
      try {
        await onAction();
      } finally {
        if (button.isConnected) {
          button.disabled = false;
          button.textContent = actionLabel;
        }
      }
    });
  }
}

const viewerTitle = document.getElementById('viewerTitle');
const viewerPath = document.getElementById('viewerPath');
const viewerOpenBtn = document.getElementById('viewerOpenBtn');
const viewerDownloadBtn = document.getElementById('viewerDownloadBtn');
const viewerSummary = document.getElementById('viewerSummary');
const viewerHeroMeta = document.getElementById('viewerHeroMeta');
const viewerSearchInput = document.getElementById('viewerSearchInput');
const viewerColumnModeSelect = document.getElementById('viewerColumnModeSelect');
const viewerClearBtn = document.getElementById('viewerClearBtn');
const viewerToolbarToggleBtn = document.getElementById('viewerToolbarToggleBtn');
const viewerWrapBtn = document.getElementById('viewerWrapBtn');
const viewerFreezeBtn = document.getElementById('viewerFreezeBtn');
const viewerWorkbookMeta = document.getElementById('viewerWorkbookMeta');
const viewerWorkbookToggleBtn = document.getElementById('viewerWorkbookToggleBtn');
const viewerWorkbookStats = document.getElementById('viewerWorkbookStats');
const viewerSheetMeta = document.getElementById('viewerSheetMeta');
const viewerSheetTabsToggleBtn = document.getElementById('viewerSheetTabsToggleBtn');
const viewerSheetSearchInput = document.getElementById('viewerSheetSearchInput');
const viewerPrevSheetBtn = document.getElementById('viewerPrevSheetBtn');
const viewerNextSheetBtn = document.getElementById('viewerNextSheetBtn');
const viewerColumnSelect = document.getElementById('viewerColumnSelect');
const viewerOperatorSelect = document.getElementById('viewerOperatorSelect');
const viewerColumnValueInput = document.getElementById('viewerColumnValueInput');
const viewerAddFilterBtn = document.getElementById('viewerAddFilterBtn');
const viewerClearFiltersBtn = document.getElementById('viewerClearFiltersBtn');
const viewerFiltersToggleBtn = document.getElementById('viewerFiltersToggleBtn');
const viewerFilterBody = document.getElementById('viewerFilterBody');
const viewerActiveFilters = document.getElementById('viewerActiveFilters');
const viewerFilterMeta = document.getElementById('viewerFilterMeta');
const viewerWindowMeta = document.getElementById('viewerWindowMeta');
const viewerWindowToggleBtn = document.getElementById('viewerWindowToggleBtn');
const viewerWindowBody = document.getElementById('viewerWindowBody');
const viewerWindowStartInput = document.getElementById('viewerWindowStartInput');
const viewerWindowSizeInput = document.getElementById('viewerWindowSizeInput');
const viewerApplyWindowBtn = document.getElementById('viewerApplyWindowBtn');
const viewerResetWindowBtn = document.getElementById('viewerResetWindowBtn');
const viewerStatus = document.getElementById('viewerStatus');
const viewerSheetTabs = document.getElementById('viewerSheetTabs');
const viewerTableWrap = document.getElementById('viewerTableWrap');
const viewerAdvancedToolbar = document.getElementById('viewerAdvancedToolbar');

let workbook = null;
let currentSheetIndex = 0;
let sortColumnIndex = null;
let sortDirection = null;
let wrapEnabled = false;
let windowEnabled = false;
let windowStart = 0;
let windowSize = 200;
let frozenColumns = 2;
let columnWidths = {};
const ROW_INDEX_WIDTH = 58;
let sheetKeyword = '';
let activeFilters = [];
let columnMode = 'auto';
let requestedSheetName = '';
let resizeFrame = null;

const viewerSectionConfig = Object.freeze({
  toolbar: {
    container: viewerAdvancedToolbar,
    button: viewerToolbarToggleBtn,
    collapsedText: '展开更多操作',
    expandedText: '收起更多操作',
  },
  workbook: {
    container: viewerWorkbookStats,
    button: viewerWorkbookToggleBtn,
    collapsedText: '展开总览',
    expandedText: '收起总览',
  },
  sheets: {
    container: viewerSheetTabs,
    button: viewerSheetTabsToggleBtn,
    collapsedText: '展开工作表列表',
    expandedText: '收起工作表列表',
  },
  filters: {
    container: viewerFilterBody,
    button: viewerFiltersToggleBtn,
    collapsedText: '展开筛选',
    expandedText: '收起筛选',
  },
  window: {
    container: viewerWindowBody,
    button: viewerWindowToggleBtn,
    collapsedText: '展开窗口',
    expandedText: '收起窗口',
  },
});

const viewerSectionCollapsedState = {
  toolbar: isCompactViewport(),
  workbook: isCompactViewport(),
  sheets: isCompactViewport(),
  filters: isCompactViewport(),
  window: isCompactViewport(),
};

function isCompactViewport() {
  return window.innerWidth <= 720;
}

function applyViewerSectionState(key) {
  const config = viewerSectionConfig[key];
  if (!config?.container) return;
  const collapsed = viewerSectionCollapsedState[key] !== false;
  config.container.classList.toggle('is-collapsed', collapsed);
  if (config.button) {
    config.button.textContent = collapsed ? config.collapsedText : config.expandedText;
    config.button.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
  }
}

function bindViewerSectionToggle(key) {
  const config = viewerSectionConfig[key];
  if (!config?.button) return;
  config.button.addEventListener('click', () => {
    viewerSectionCollapsedState[key] = !viewerSectionCollapsedState[key];
    applyViewerSectionState(key);
  });
  applyViewerSectionState(key);
}

function formatSortStatus(sheet) {
  if (sortColumnIndex === null || !sheet?.columns?.length) return '未排序';
  const columnName = sheet.columns[sortColumnIndex] || `第 ${sortColumnIndex + 1} 列`;
  return `${columnName} · ${sortDirection === 'desc' ? '降序' : '升序'}`;
}

function setSummary(sheet, filteredRowCount) {
  const activeSheet = sheet || { name: '-', row_count: 0, column_count: 0 };
  viewerSummary.innerHTML = `
    <div><strong>文件名</strong><span>${escapeHtml(workbook?.filename || '-')}</span></div>
    <div><strong>工作表数</strong><span>${workbook?.sheet_count ?? '-'}</span></div>
    <div><strong>当前工作表</strong><span>${escapeHtml(activeSheet.name)}</span></div>
    <div><strong>总行数</strong><span>${activeSheet.row_count ?? '-'}</span></div>
    <div><strong>筛选后行数</strong><span>${filteredRowCount ?? '-'}</span></div>
    <div><strong>当前列数</strong><span>${activeSheet.column_count ?? '-'}</span></div>
    <div><strong>排序状态</strong><span>${escapeHtml(formatSortStatus(activeSheet))}</span></div>
  `;
  if (viewerHeroMeta) {
    viewerHeroMeta.innerHTML = `
      <div class="stat-card">
        <strong>当前工作表</strong>
        <span>${escapeHtml(activeSheet.name)}</span>
        <small>${activeSheet.column_count ?? '-'} 列</small>
      </div>
      <div class="stat-card">
        <strong>匹配行数</strong>
        <span>${escapeHtml(filteredRowCount ?? '-')}</span>
        <small>当前筛选结果</small>
      </div>
      <div class="stat-card">
        <strong>工作表总数</strong>
        <span>${escapeHtml(workbook?.sheet_count ?? '-')}</span>
        <small>当前文件</small>
      </div>
    `;
  }
}

function filterRows(sheet, keyword) {
  const indexedRows = sheet.rows.map((row, index) => ({ row, index }));
  if (!keyword) return indexedRows;
  const needle = keyword.toLowerCase();
  return indexedRows.filter(({ row }) => row.some((cell) => String(cell ?? '').toLowerCase().includes(needle)));
}

function parseSortableValue(value) {
  const text = String(value ?? '').trim();
  if (!text) return { raw: text, numeric: null };
  const normalized = text.replaceAll(',', '').replace(/%$/, '');
  const numeric = Number(normalized);
  return { raw: text, numeric: Number.isFinite(numeric) ? numeric : null };
}

function numericClass(value, header) {
  const text = String(value ?? '').trim();
  if (!text) return '';
  const numeric = Number(text.replaceAll(',', '').replace(/%$/, ''));
  if (!Number.isFinite(numeric)) return '';
  const title = String(header || '');
  const isMetricColumn = title.includes('(%)') || title.includes('得分') || title.includes('比率') || title.includes('VaR');
  if (!isMetricColumn) return '';
  if (numeric > 0) return 'num-pos';
  if (numeric < 0) return 'num-neg';
  return 'num-zero';
}

function getColumnWidth(index) {
  return Number(columnWidths[index] || 140);
}

function columnWidthStyle(index) {
  const width = getColumnWidth(index);
  return `width:${width}px;min-width:${width}px;max-width:${width}px;`;
}

function applyColumnWidth(index) {
  const widthStyle = columnWidthStyle(index);
  viewerTableWrap.querySelectorAll(`[data-column-index="${index}"]`).forEach((element) => {
    element.style.width = `${getColumnWidth(index)}px`;
    element.style.minWidth = `${getColumnWidth(index)}px`;
    element.style.maxWidth = `${getColumnWidth(index)}px`;
  });
  return widthStyle;
}

function scheduleColumnWidthApply(index) {
  if (resizeFrame !== null) {
    window.cancelAnimationFrame(resizeFrame);
  }
  resizeFrame = window.requestAnimationFrame(() => {
    resizeFrame = null;
    applyColumnWidth(index);
  });
}

function stickyStyleForDataColumn(colIndex) {
  if (colIndex >= frozenColumns) return '';
  let left = ROW_INDEX_WIDTH;
  for (let i = 0; i < colIndex; i += 1) {
    left += getColumnWidth(i);
  }
  return `left:${left}px;z-index:${4 - Math.min(colIndex, 2)};`;
}

function sortRows(rows) {
  if (sortColumnIndex === null || !sortDirection) return rows;
  const direction = sortDirection === 'desc' ? -1 : 1;
  return [...rows].sort((left, right) => {
    const leftValue = parseSortableValue(left.row[sortColumnIndex]);
    const rightValue = parseSortableValue(right.row[sortColumnIndex]);

    if (leftValue.numeric !== null && rightValue.numeric !== null) {
      if (leftValue.numeric === rightValue.numeric) return left.index - right.index;
      return (leftValue.numeric - rightValue.numeric) * direction;
    }

    const compared = leftValue.raw.localeCompare(rightValue.raw, 'zh-CN', {
      numeric: true,
      sensitivity: 'base',
    });
    return compared === 0 ? left.index - right.index : compared * direction;
  });
}

function sortIndicator(columnIndex) {
  if (sortColumnIndex !== columnIndex) return '<span class="sort-indicator">⇅</span>';
  return `<span class="sort-indicator active">${sortDirection === 'desc' ? '↓' : '↑'}</span>`;
}

function getVisibleSheets() {
  const sheets = workbook?.sheets || [];
  if (!sheetKeyword) return sheets;
  const keyword = sheetKeyword.toLowerCase();
  return sheets.filter((sheet) => String(sheet.name || '').toLowerCase().includes(keyword));
}

function currentSheet() {
  return workbook?.sheets?.[currentSheetIndex] || null;
}

function syncSheetQuery() {
  const sheet = currentSheet();
  if (!sheet?.name) return;
  const params = new URLSearchParams(window.location.search);
  params.set('sheet', sheet.name);
  window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}`);
}

function resolveVisibleColumnIndexes(sheet) {
  const columns = sheet?.columns || [];
  if (!columns.length) return [];

  if (columnMode === 'all') {
    return columns.map((_, index) => index);
  }

  const presets = {
    basic: ['基金代码', '基金名称', '基金类型', '风险等级', '成立日期', '基金规模(亿)', '基金公司'],
    returns: ['基金代码', '基金名称', '近1周(%)', '近1月(%)', '近3月(%)', '近6月(%)', '近1年(%)', '正式投资评分', '近期得分', '长期得分'],
    validation: ['基金代码', '基金名称', '母基金名称', '关联份额数', '关联份额名称', '关联份额代码',
      ...(columns.some((column) => FUND_DATA_VALIDATION_FIELDS.includes(column))
        ? FUND_DATA_VALIDATION_FIELDS : ['AutoStock校验状态', 'AutoStock校验备注'])],
  };

  const preferred = presets[columnMode];
  if (preferred) {
    return preferred.map((name) => columns.indexOf(name)).filter((index) => index >= 0);
  }

  if (columns.length <= 12) {
    return columns.map((_, index) => index);
  }

  const autoPreferred = [
    '基金代码',
    '基金名称',
    '基金类型',
    '风险等级',
    '基金规模(亿)',
    '近1月(%)',
    '近3月(%)',
    '近1年(%)',
    '正式投资评分',
    '优选类别',
    ...(columns.some((column) => FUND_DATA_VALIDATION_FIELDS.includes(column))
      ? ['数据校验状态', '数据校验来源'] : ['AutoStock校验状态']),
  ];
  const indexes = autoPreferred
    .map((name) => columns.indexOf(name))
    .filter((index) => index >= 0);
  return indexes.length ? indexes : columns.slice(0, 10).map((_, index) => index);
}

function populateColumnSelect() {
  const sheet = currentSheet();
  const options = sheet?.columns || [];
  viewerColumnSelect.innerHTML = options
    .map((column, index) => `<option value="${index}">${escapeHtml(column)}</option>`)
    .join('');
}

function renderActiveFilters() {
  if (!activeFilters.length) {
    viewerActiveFilters.innerHTML = '<div class="empty-state-inline">暂无规则</div>';
    viewerFilterMeta.textContent = '支持多条规则';
    return;
  }

  viewerFilterMeta.textContent = `已启用 ${activeFilters.length} 条`;
  viewerActiveFilters.innerHTML = activeFilters
    .map(
      (rule, index) => `
        <div class="active-filter-chip">
          <span>${escapeHtml(rule.columnLabel)} · ${escapeHtml(rule.operatorLabel)}${rule.value ? ` · ${escapeHtml(rule.value)}` : ''}</span>
          <button type="button" class="filter-remove-btn" data-filter-index="${index}">移除</button>
        </div>
      `
    )
    .join('');

  viewerActiveFilters.querySelectorAll('[data-filter-index]').forEach((button) => {
    button.addEventListener('click', () => {
      activeFilters.splice(Number(button.dataset.filterIndex), 1);
      renderActiveFilters();
      renderTable();
    });
  });
}

function matchFilter(cellValue, rule) {
  const text = String(cellValue ?? '').trim();
  const lower = text.toLowerCase();
  const value = String(rule.value ?? '').trim();
  const lowerValue = value.toLowerCase();
  const numeric = Number(text.replaceAll(',', '').replace(/%$/, ''));
  const targetNumeric = Number(value.replaceAll(',', '').replace(/%$/, ''));

  switch (rule.operator) {
    case 'contains':
      return lower.includes(lowerValue);
    case 'equals':
      return text === value;
    case 'not_equals':
      return text !== value;
    case 'starts_with':
      return lower.startsWith(lowerValue);
    case 'ends_with':
      return lower.endsWith(lowerValue);
    case 'gt':
      return Number.isFinite(numeric) && Number.isFinite(targetNumeric) && numeric > targetNumeric;
    case 'gte':
      return Number.isFinite(numeric) && Number.isFinite(targetNumeric) && numeric >= targetNumeric;
    case 'lt':
      return Number.isFinite(numeric) && Number.isFinite(targetNumeric) && numeric < targetNumeric;
    case 'lte':
      return Number.isFinite(numeric) && Number.isFinite(targetNumeric) && numeric <= targetNumeric;
    case 'empty':
      return text === '';
    case 'not_empty':
      return text !== '';
    default:
      return true;
  }
}

function applyColumnFilters(rows) {
  if (!activeFilters.length) return rows;
  return rows.filter(({ row }) => activeFilters.every((rule) => matchFilter(row[rule.columnIndex], rule)));
}

function renderWorkbookStats() {
  if (!workbook?.sheets?.length) {
    viewerWorkbookMeta.textContent = '暂无信息';
    viewerWorkbookStats.innerHTML = '';
    return;
  }

  const totalRows = workbook.sheets.reduce((sum, sheet) => sum + Number(sheet.row_count || 0), 0);
  const totalCols = workbook.sheets.reduce((sum, sheet) => sum + Number(sheet.column_count || 0), 0);
  const maxRowsSheet = [...workbook.sheets].sort((a, b) => Number(b.row_count || 0) - Number(a.row_count || 0))[0];

  viewerWorkbookMeta.textContent = `${workbook.sheet_count} 个工作表 / ${payloadSourceContractSummary(workbook)}`;
  viewerWorkbookStats.innerHTML = `
    <div class="stat-card"><strong>工作表数</strong><span>${workbook.sheet_count}</span><small>当前文件</small></div>
    <div class="stat-card"><strong>总行数</strong><span>${totalRows}</span><small>合计</small></div>
    <div class="stat-card"><strong>总列数</strong><span>${totalCols}</span><small>合计</small></div>
    <div class="stat-card"><strong>最大工作表</strong><span>${escapeHtml(maxRowsSheet?.name || '-')}</span><small>${maxRowsSheet?.row_count || 0} 行</small></div>
    <div class="stat-card"><strong>文件名</strong><span>${escapeHtml(workbook.filename || '-')}</span><small>预览目标</small></div>
    <div class="stat-card"><strong>路径</strong><span>${escapeHtml(workbook.path || '-')}</span><small>项目路径</small></div>
  `;
}

function applyWindow(rows) {
  if (!windowEnabled) {
    viewerWindowMeta.textContent = '显示全部';
    return rows;
  }
  const start = Math.max(0, windowStart);
  const end = Math.max(start, start + windowSize);
  viewerWindowMeta.textContent = `${start + 1}-${Math.min(end, rows.length)} / ${rows.length}`;
  return rows.slice(start, end);
}

function renderTable() {
  if (!workbook || !workbook.sheets?.length) {
    viewerTableWrap.innerHTML = '<div class="excel-preview-empty">暂无可预览工作表</div>';
    return;
  }

  const sheet = workbook.sheets[currentSheetIndex];
  const visibleColumnIndexes = resolveVisibleColumnIndexes(sheet);
  const keyword = viewerSearchInput.value.trim();
  const sortedRows = sortRows(applyColumnFilters(filterRows(sheet, keyword)));
  const rows = applyWindow(sortedRows);
  setSummary(sheet, sortedRows.length);
  viewerStatus.textContent = keyword
    ? `匹配 ${sortedRows.length} 行`
    : `共 ${sortedRows.length} 行`;
  viewerFilterMeta.textContent = `${activeFilters.length ? `已启用 ${activeFilters.length} 条` : '支持多条规则'}；显示 ${visibleColumnIndexes.length} / ${sheet.columns.length} 列`;

  const headerHtml = [
    `<th class="sticky-col sticky-col-0" style="left:0;min-width:${ROW_INDEX_WIDTH}px;width:${ROW_INDEX_WIDTH}px;">#</th>`,
    ...visibleColumnIndexes.map(
      (index, visibleIndex) => `
        <th
          class="${visibleIndex < frozenColumns ? 'sticky-col' : ''}"
          data-column-index="${index}"
          style="${visibleIndex < frozenColumns ? stickyStyleForDataColumn(visibleIndex) : ''}${columnWidthStyle(index)}"
        >
          <div class="resizable-header">
            <button class="table-sort-btn" type="button" data-sort-index="${index}">
            <span>${escapeHtml(sheet.columns[index])}</span>
            ${sortIndicator(index)}
            </button>
            <span class="col-resizer" data-resize-index="${index}"></span>
          </div>
        </th>
      `
    ),
  ].join('');

  const bodyHtml = rows.length
    ? rows
        .map(
          ({ row, index }, rowIndex) => `
            <tr>
              <td class="sticky-col sticky-col-0 row-index" style="left:0;min-width:${ROW_INDEX_WIDTH}px;width:${ROW_INDEX_WIDTH}px;" title="原始行号：${index + 1}">${rowIndex + 1}</td>
              ${visibleColumnIndexes
                .map((colIndex, visibleIndex) => {
                  const originalCell = row[colIndex];
                  const cell = sheet.columns[colIndex] === '数据校验来源'
                    ? publicationValidationProviderLabel(originalCell) : originalCell;
                  const stickyClass = visibleIndex < frozenColumns ? 'sticky-col' : '';
                  const colorClass = numericClass(cell, sheet.columns[colIndex]);
                  const stickyStyle = stickyClass ? `${stickyStyleForDataColumn(visibleIndex)}` : '';
                  const finalStyle = `${stickyStyle}${columnWidthStyle(colIndex)}`;
                  return `<td class="${stickyClass} ${colorClass}" data-column-index="${colIndex}" ${finalStyle ? `style="${finalStyle}"` : ''} title="${escapeHtml(cell)}">${highlightText(cell, keyword)}</td>`;
                })
                .join('')}
            </tr>
          `
        )
        .join('')
    : `<tr><td class="sticky-col row-index">1</td><td colspan="${Math.max(sheet.columns.length, 1)}">没有匹配数据</td></tr>`;

  viewerTableWrap.innerHTML = `
    <div class="table-wrap viewer-table-wrap">
      <table class="preview-table">
        <thead><tr>${headerHtml}</tr></thead>
        <tbody>${bodyHtml}</tbody>
      </table>
    </div>
  `;
  syncTableWrapStickyOffsets(viewerTableWrap);

  viewerTableWrap.querySelectorAll('.table-sort-btn').forEach((button) => {
    button.addEventListener('click', () => {
      const columnIndex = Number(button.dataset.sortIndex);
      if (sortColumnIndex !== columnIndex) {
        sortColumnIndex = columnIndex;
        sortDirection = 'asc';
      } else if (sortDirection === 'asc') {
        sortDirection = 'desc';
      } else {
        sortColumnIndex = null;
        sortDirection = null;
      }
      renderTable();
    });
  });

  viewerTableWrap.querySelectorAll('.col-resizer').forEach((resizer) => {
    resizer.addEventListener('mousedown', (event) => {
      event.preventDefault();
      const index = Number(resizer.dataset.resizeIndex);
      const th = resizer.closest('th');
      const startX = event.clientX;
      const startWidth = th?.offsetWidth || 140;

      function onMove(moveEvent) {
        columnWidths[index] = Math.max(80, startWidth + moveEvent.clientX - startX);
        scheduleColumnWidthApply(index);
      }

      function onUp() {
        window.removeEventListener('mousemove', onMove);
        window.removeEventListener('mouseup', onUp);
        if (resizeFrame !== null) {
          window.cancelAnimationFrame(resizeFrame);
          resizeFrame = null;
        }
        applyColumnWidth(index);
      }

      window.addEventListener('mousemove', onMove);
      window.addEventListener('mouseup', onUp);
    });
  });
}

function renderSheetTabs() {
  if (!workbook?.sheets?.length) {
    viewerSheetTabs.innerHTML = '';
    return;
  }

  const visibleSheets = getVisibleSheets();
  viewerSheetMeta.textContent = `显示 ${visibleSheets.length} / ${workbook.sheets.length} 个工作表`;
  viewerSheetTabs.innerHTML = visibleSheets
    .map(
      (sheet) => `
        <button class="sheet-tab ${workbook.sheets[currentSheetIndex]?.name === sheet.name ? 'active' : ''}" type="button" data-sheet-name="${escapeHtml(sheet.name)}">
          ${escapeHtml(sheet.name)} (${sheet.row_count} 行)
        </button>
      `
    )
    .join('');

  viewerSheetTabs.querySelectorAll('.sheet-tab').forEach((button) => {
    button.addEventListener('click', () => {
      currentSheetIndex = workbook.sheets.findIndex((sheet) => sheet.name === button.dataset.sheetName);
      sortColumnIndex = null;
      sortDirection = null;
      activeFilters = [];
      populateColumnSelect();
      renderActiveFilters();
      syncSheetQuery();
      renderSheetTabs();
      renderTable();
    });
  });
}

async function initViewer() {
  const path = getPathParam();
  requestedSheetName = getSheetParam();
  if (!path) {
    viewerPath.textContent = '缺少文件路径';
    renderViewerFailureState({
      title: '缺少 Excel 文件路径',
      message: '未提供 path 参数，无法加载预览内容。',
      linkLabel: '返回文件页',
      linkHref: '/reports',
    });
    return;
  }

  viewerPath.textContent = path;
  viewerOpenBtn.href = fileUrl(path, false);
  viewerDownloadBtn.href = fileUrl(path, true);

  const normalizedPath = normalizeProjectPath(path);
  let res;
  let data;
  try {
    res = await fetch(`/api/preview-excel?path=${encodeURIComponent(normalizedPath)}`);
    data = await res.json();
  } catch (error) {
    renderViewerFailureState({
      title: 'Excel 预览加载失败',
      message: error?.message || '预览接口暂不可用',
      actionLabel: '重新加载',
      onAction: initViewer,
      linkLabel: '返回文件页',
      linkHref: '/reports',
    });
    return;
  }
  if (!res.ok) {
    renderViewerFailureState({
      title: 'Excel 预览加载失败',
      message: data.error || '加载失败',
      actionLabel: '重新加载',
      onAction: initViewer,
      linkLabel: '返回文件页',
      linkHref: '/reports',
    });
    return;
  }

  workbook = data;
  applyPayloadDataContract(workbook);
  if (isCompactViewport()) {
    columnMode = 'basic';
    viewerColumnModeSelect.value = 'basic';
    wrapEnabled = true;
    document.body.classList.add('wrap-cells');
    viewerWrapBtn.textContent = '换行关';
    frozenColumns = 1;
    viewerFreezeBtn.textContent = '冻2列';
    if (!viewerWindowSizeInput.value || Number(viewerWindowSizeInput.value) > 120) {
      viewerWindowSizeInput.value = 80;
    }
  }
  if (requestedSheetName) {
    const matchedIndex = workbook.sheets.findIndex((sheet) => sheet.name === requestedSheetName);
    if (matchedIndex >= 0) {
      currentSheetIndex = matchedIndex;
    }
  }
  document.title = `${data.filename} - Excel 预览`;
  viewerTitle.textContent = data.filename;
  viewerPath.textContent = data.path;
  renderWorkbookStats();
  populateColumnSelect();
  renderActiveFilters();
  renderSheetTabs();
  renderTable();
}

Object.keys(viewerSectionConfig).forEach((key) => {
  bindViewerSectionToggle(key);
});

viewerSearchInput.addEventListener('input', () => {
  renderTable();
});

viewerClearBtn.addEventListener('click', () => {
  viewerSearchInput.value = '';
  renderTable();
});

viewerColumnModeSelect.addEventListener('change', () => {
  columnMode = viewerColumnModeSelect.value || 'auto';
  renderTable();
});

viewerWrapBtn.addEventListener('click', () => {
  wrapEnabled = !wrapEnabled;
  document.body.classList.toggle('wrap-cells', wrapEnabled);
  viewerWrapBtn.textContent = wrapEnabled ? '换行关' : '换行开';
});

viewerFreezeBtn.addEventListener('click', () => {
  frozenColumns = frozenColumns === 2 ? 1 : 2;
  viewerFreezeBtn.textContent = frozenColumns === 2 ? '冻1列' : '冻2列';
  renderTable();
});

viewerSheetSearchInput.addEventListener('input', () => {
  sheetKeyword = String(viewerSheetSearchInput.value || '').trim();
  renderSheetTabs();
});

viewerPrevSheetBtn.addEventListener('click', () => {
  if (!workbook?.sheets?.length) return;
  currentSheetIndex = (currentSheetIndex - 1 + workbook.sheets.length) % workbook.sheets.length;
  sortColumnIndex = null;
  sortDirection = null;
  activeFilters = [];
  populateColumnSelect();
  renderActiveFilters();
  syncSheetQuery();
  renderSheetTabs();
  renderTable();
});

viewerNextSheetBtn.addEventListener('click', () => {
  if (!workbook?.sheets?.length) return;
  currentSheetIndex = (currentSheetIndex + 1) % workbook.sheets.length;
  sortColumnIndex = null;
  sortDirection = null;
  activeFilters = [];
  populateColumnSelect();
  renderActiveFilters();
  syncSheetQuery();
  renderSheetTabs();
  renderTable();
});

viewerAddFilterBtn.addEventListener('click', () => {
  const sheet = currentSheet();
  if (!sheet?.columns?.length) return;
  const columnIndex = Number(viewerColumnSelect.value || 0);
  const operator = viewerOperatorSelect.value || 'contains';
  const value = viewerColumnValueInput.value || '';
  const operatorLabel = viewerOperatorSelect.options[viewerOperatorSelect.selectedIndex]?.text || operator;

  activeFilters.push({
    columnIndex,
    columnLabel: sheet.columns[columnIndex] || `第 ${columnIndex + 1} 列`,
    operator,
    operatorLabel,
    value,
  });
  renderActiveFilters();
  renderTable();
});

viewerClearFiltersBtn.addEventListener('click', () => {
  activeFilters = [];
  viewerColumnValueInput.value = '';
  renderActiveFilters();
  renderTable();
});

viewerApplyWindowBtn.addEventListener('click', () => {
  const startValue = Math.max(1, Number(viewerWindowStartInput.value || 1));
  const sizeValue = Math.max(20, Number(viewerWindowSizeInput.value || 200));
  windowEnabled = true;
  windowStart = startValue - 1;
  windowSize = sizeValue;
  renderTable();
});

viewerResetWindowBtn.addEventListener('click', () => {
  windowEnabled = false;
  windowStart = 0;
  windowSize = Number(viewerWindowSizeInput.value || 200);
  viewerWindowStartInput.value = 1;
  renderTable();
});

initViewer();
