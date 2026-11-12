const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const shared = fs.readFileSync(path.join(__dirname, '../frontend/shared.js'), 'utf8');
function declaration(name) {
  const start = shared.indexOf(`function ${name}(`);
  assert.ok(start >= 0);
  const end = shared.indexOf('\nfunction ', start + 1);
  assert.ok(end > start);
  return shared.slice(start, end);
}
const context = vm.createContext({});
vm.runInContext(`${declaration('escapeHtml')}\n${declaration('renderCell')}`, context);

test('purchase flags render consistently across boolean, numeric and report forms', () => {
  for (const value of [true, 1, '1.0', 'True', '可申购', '开放申购']) {
    const rendered = context.renderCell('是否可申购', value);
    assert.match(rendered, /pill-yes/);
    assert.match(rendered, />可申购</);
  }
  for (const value of [false, 0, '0.0', 'False', '不可申购', '暂停申购']) {
    const rendered = context.renderCell('是否可申购', value);
    assert.match(rendered, /pill-no/);
    assert.match(rendered, />不可申购</);
  }
});

test('missing and unknown purchase status never receive an affirmative or negative badge', () => {
  for (const value of [null, undefined, '', '申购状态未知']) {
    const rendered = context.renderCell('是否可申购', value);
    assert.doesNotMatch(rendered, /pill-yes|pill-no/);
    assert.doesNotMatch(rendered, />(?:可申购|不可申购)</);
  }
  assert.match(context.renderCell('是否可申购', '申购状态未知'), />申购状态未知</);
});
