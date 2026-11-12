// Exercise the shipped browser filters with synthetic NULL/zero input markers.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
function read(name) { return fs.readFileSync(path.join(root, 'frontend', name), 'utf8'); }
function extract(source, name, nextName) {
  return source.slice(source.indexOf(`function ${name}(`), source.indexOf(`function ${nextName}(`));
}
const context = vm.createContext({buildRecommendationQualityFlags: () => []});
vm.runInContext(extract(read('shared.js'), 'parseMetricDisplayNumber', 'formatMetricDisplayValue') +
  extract(read('tracker.js'), 'isTrackerValuationMarkedMissing', 'bindTrackerFilterBar') +
  extract(read('recommendations.js'), 'summarizeGroupNarrative', 'renderRecommendationGroupNarrative') +
  extract(read('recommendations.js'), 'filterRecommendationFunds', 'renderRecommendationFilterBar'), context);
for (const value of [null, undefined, '', '  ', 'bad']) {
  assert.equal(context.parseMetricDisplayNumber(value), null);
  for (const strength of ['strong', 'weak']) {
    assert.equal(context.filterTrackerRows([{'基金代码': 'fixture', '近3月(%)': value}], {recentStrength: strength}).length, 0);
  }
  assert.equal(context.filterRecommendationFunds([{'基金代码': 'fixture', '正式投资评分': value}], {scoreFloor: '0'}).length, 0);
  assert.equal(context.filterRecommendationFunds([{'基金代码': 'fixture', '平台成立以来收益年化(%)': value}], {annualizedAvailability: 'available'}).length, 0);
}
assert.equal(context.parseMetricDisplayNumber(0), 0);
assert.equal(context.filterTrackerRows([{'基金代码': 'fixture', '近3月(%)': 0}], {recentStrength: 'weak'}).length, 1);
assert.equal(context.filterTrackerRows([{'基金代码': 'fixture', '正式投资评分': null}], {scoreFloor: '1'}).length, 0);
assert.equal(context.filterRecommendationFunds([{'基金代码': 'fixture', '平台成立以来收益年化(%)': 0}], {annualizedAvailability: 'available'}).length, 1);
assert.equal(context.filterRecommendationFunds([{'基金代码': 'fixture', '正式投资评分': 0}], {scoreFloor: '0'}).length, 1);
assert.equal(context.summarizeGroupNarrative({count: 5}).headline, '明细尚未加载');
assert.equal(context.summarizeGroupNarrative({count: 0, funds: []}).headline, '暂无基金');
console.log('Frontend NULL/zero and summary/detail states passed');
