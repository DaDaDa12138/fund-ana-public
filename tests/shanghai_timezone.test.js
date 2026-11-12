const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {test} = require('node:test');
const source = fs.readFileSync(path.join(__dirname, '../frontend/shared.js'), 'utf8');
const code = source.slice(source.indexOf('function formatDateTime('), source.indexOf('function formatNumber('));
test('timestamps render in Shanghai independently of browser location', () => {
  const previous = process.env.TZ;
  try {
    for (const location of ['UTC', 'America/New_York', 'Asia/Shanghai']) {
      process.env.TZ = location;
      const context = vm.createContext({});
      vm.runInContext(code, context);
      const format = value => context.formatDateTime(value);
      assert.match(format('2026-11-08T23:47:00Z'), /2026\/11\/09 07:47:00/);
      assert.equal(format('2026-11-09 07:47:00'), format('2026-11-08T23:47:00Z'));
      assert.equal(format('2026-11-09T07:47:00+08:00'), format('2026-11-08T23:47:00Z'));
      assert.match(format('2026-11-09'), /2026\/11\/09 00:00:00/);
      assert.equal(format(null), '-');
      assert.equal(format('invalid'), 'invalid');
    }
  } finally {
    if (previous === undefined) delete process.env.TZ;
    else process.env.TZ = previous;
  }
});
