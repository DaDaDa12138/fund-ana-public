#!/usr/bin/env python3
"""Configure an ephemeral runner from reviewed, credential-free templates."""
from pathlib import Path
import secrets
import sys
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> None:
    from backend.github_cloud_state import CONFIG_FILES
    if any(not (ROOT / name).is_file() for name in CONFIG_FILES):
        raise RuntimeError('cloud_operational_policy_missing')
    project = yaml.safe_load((ROOT / 'config/project.example.yaml').read_text())
    project['project']['timezone'] = 'Asia/Shanghai'
    project['scheduler']['timezone'] = 'Asia/Shanghai'
    # The scheduler's enabled flag also authorizes run_once. Keep it enabled
    # for the external one-shot process; the web container explicitly disables
    # its embedded scheduler and no resident scheduler container is started.
    project['scheduler']['enabled'] = True
    project['scheduler']['host_mode'] = 'external_process'
    project['scheduler']['snapshot_watchdog']['enabled'] = False
    project['scheduler']['storage_observation']['enabled'] = False
    project['scheduler']['storage_lifecycle']['enabled'] = False
    # Data maintenance remains cloud-scheduled. Host disk/backup migrations
    # must not run against an ephemeral Runner or produce redundant full DBs.
    essential = {'plain_open_tail_follow_up', 'future_outlier_monitor',
                 'holding_period_edge_audit', 'nav_history_consistency_audit'}
    for name, options in project['scheduler']['weekly_maintenance'].items():
        if isinstance(options, dict) and 'enabled' in options and name not in essential:
            options['enabled'] = False
    (ROOT / 'config/project.yaml').write_text(yaml.safe_dump(project, allow_unicode=True, sort_keys=False))
    core = yaml.safe_load((ROOT / 'core/config/config.yaml.template').read_text())
    core.setdefault('web', {})['secret_key'] = secrets.token_hex(32)
    (ROOT / 'core/config/config.yaml').write_text(yaml.safe_dump(core, allow_unicode=True, sort_keys=False))


if __name__ == '__main__': main()
