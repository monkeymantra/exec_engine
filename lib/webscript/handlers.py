def run_webscript(event):
    run_id = event.run_id
    from super_user.models import WebScriptRun
    run = WebScriptRun.objects.get(id=run_id)
    output = run.run()
    return output
