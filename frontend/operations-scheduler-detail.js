(function () {
  const OPERATIONS_SCHEDULER_DETAIL_SECTIONS = Object.freeze([
    'runs',
    'manual_history',
    'task_closures',
    'maintenance',
    'schedule_stability',
    'long_term_governance',
  ]);

  const OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS = Object.freeze({
    schedulerSecondaryStats: ['runs', 'schedule_stability'],
    manualRunHistory: ['manual_history'],
    regressionSmokeArtifacts: ['maintenance'],
    longTermGovernanceQueue: ['long_term_governance'],
  });

  function normalizeOperationsSchedulerSections(sections) {
    const available = new Set(OPERATIONS_SCHEDULER_DETAIL_SECTIONS);
    return [...new Set((Array.isArray(sections) ? sections : [])
      .map((section) => String(section || '').trim())
      .filter((section) => available.has(section)))];
  }

  function hasOperationsSchedulerSectionData(detailPayload, section) {
    const key = String(section || '').trim();
    const loaded = new Set(detailPayload?.loaded_sections || []);
    if (loaded.has(key)) return true;
    if (key === 'runs') {
      return Boolean(detailPayload?.last_runs || detailPayload?.latest_runs);
    }
    if (key === 'manual_history') {
      return Boolean(
        Array.isArray(detailPayload?.manual_run_history)
        || Array.isArray(detailPayload?.pending_manual_run_requests)
      );
    }
    if (key === 'task_closures') {
      return Boolean(detailPayload?.task_closures || detailPayload?.child_task_idle_streaks);
    }
    if (key === 'maintenance') {
      return Boolean(detailPayload?.maintenance_summary || detailPayload?.task_run_artifacts);
    }
    if (key === 'schedule_stability') {
      return Boolean(detailPayload?.schedule_stability_summary || detailPayload?.jobs);
    }
    if (key === 'long_term_governance') {
      return Boolean(
        detailPayload?.long_term_governance_summary
        || detailPayload?.current_final_selection_governance_queue
      );
    }
    return false;
  }

  function mergeOperationsSchedulerDetailSections(sectionResults) {
    const merged = {
      detail_scope: 'section_bundle',
      detail_bundle_schema_version: 1,
      loaded_sections: [],
      section_errors: [],
    };
    sectionResults.forEach((item) => {
      const section = String(item?.section || item?.payload?.section || '').trim();
      const payload = item?.payload && typeof item.payload === 'object' ? item.payload : {};
      if (!section || !Object.keys(payload).length) return;
      merged.loaded_sections.push(section);
      Object.entries(payload).forEach(([key, value]) => {
        if (key === 'section' || key === 'detail_scope' || key === 'generated_from') return;
        if (value !== undefined) {
          merged[key] = value;
        }
      });
    });
    merged.loaded_sections = normalizeOperationsSchedulerSections(merged.loaded_sections);
    return merged;
  }

  function mergeOperationsSchedulerDetailPayloads(existingPayload, nextPayload) {
    const existing = existingPayload && typeof existingPayload === 'object' ? existingPayload : {};
    const next = nextPayload && typeof nextPayload === 'object' ? nextPayload : {};
    const loadedSections = [
      ...(Array.isArray(existing.loaded_sections) ? existing.loaded_sections : []),
      ...(Array.isArray(next.loaded_sections) ? next.loaded_sections : []),
    ];
    const sectionErrors = [
      ...(Array.isArray(existing.section_errors) ? existing.section_errors : []),
      ...(Array.isArray(next.section_errors) ? next.section_errors : []),
    ];
    return {
      ...existing,
      ...next,
      detail_scope: 'section_bundle',
      detail_bundle_schema_version: 1,
      loaded_sections: normalizeOperationsSchedulerSections(loadedSections),
      section_errors: sectionErrors,
    };
  }

  async function loadOperationsSchedulerDetailSections(sections, options = {}) {
    const loader = options.loadSchedulerStatusDetail || window.loadSchedulerStatusDetail;
    if (typeof loader !== 'function') {
      throw new Error('调度详情加载器不可用');
    }
    const requestedSections = normalizeOperationsSchedulerSections(sections);
    if (!requestedSections.length) {
      return {
        detail_scope: 'section_bundle',
        detail_bundle_schema_version: 1,
        loaded_sections: [],
        section_errors: [],
      };
    }
    const requests = requestedSections.map((section) => (
      loader({ section, sampleLimit: 0 })
        .then((payload) => ({ section, payload }))
    ));
    const results = await Promise.allSettled(requests);
    const fulfilled = [];
    const failures = [];
    results.forEach((result, index) => {
      const section = requestedSections[index];
      if (result.status === 'fulfilled') {
        fulfilled.push(result.value);
        return;
      }
      failures.push({
        section,
        message: result.reason?.message || String(result.reason || 'section unavailable'),
      });
    });
    if (!fulfilled.length) {
      throw new Error('调度分块详情暂不可用');
    }
    const bundle = mergeOperationsSchedulerDetailSections(fulfilled);
    bundle.section_errors = failures;
    return bundle;
  }

  window.OperationsSchedulerDetail = Object.freeze({
    OPERATIONS_SCHEDULER_DETAIL_SECTIONS,
    OPERATIONS_DETAIL_SECTION_SCHEDULER_SECTIONS,
    normalizeOperationsSchedulerSections,
    hasOperationsSchedulerSectionData,
    mergeOperationsSchedulerDetailSections,
    mergeOperationsSchedulerDetailPayloads,
    loadOperationsSchedulerDetailSections,
  });
}());
