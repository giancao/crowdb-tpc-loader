import pytest

from crowdb_tpc_loader.journal_workers import journal_workers


@pytest.mark.parametrize("failure", [OSError("journal is full"), KeyboardInterrupt()])
def test_failed_checkpoint_releases_workers_and_prevents_remote_effects(failure):
    effects = []

    def worker(name, value, checkpoint):
        checkpoint({"state": "before-upload"})
        effects.append(name)

    def save(*_):
        raise failure

    with pytest.raises(type(failure)):
        journal_workers([(str(i), i) for i in range(8)], worker, save, 2)
    assert effects == []
