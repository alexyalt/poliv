import json
import sys
import pytest
from server.tools.test_wizard import Report, Processes


def test_answer_validation_checkpoint_resume(tmp_path, monkeypatch):
    r=Report(tmp_path/'report','software')
    answers=iter(['maybe','да','nan','-2','1,5','первая строка','вторая строка','.'])
    monkeypatch.setattr('builtins.input',lambda *_:next(answers))
    assert r.ask('yes','Согласны?')=='да'
    assert r.ask('number','Сколько?','number')=='1,5'
    assert r.ask('text','Опишите','text')=='первая строка\nвторая строка'
    restored=Report(r.folder,'software',True)
    assert restored.ask('yes','Согласны?')=='да'
    assert len(json.loads(r.path.read_text())['answers'])==3
    assert 'первая строка' in (r.folder/'report.md').read_text()
    with pytest.raises(ValueError):Report(r.folder,'controller',True)


def test_owned_process_stops_and_logs(tmp_path):
    r=Report(tmp_path/'report','software')
    p=Processes(r)
    child=p.start('child',[sys.executable,'-c','import time;time.sleep(30)'])
    p.close()
    assert child.poll() is not None
    assert (r.folder/'child.log').exists()
