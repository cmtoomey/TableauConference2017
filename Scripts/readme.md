# FYI - or how to use Conf.yaml and virtualenv

Each script is set up to be independent of the others, but all depend on conf.yaml. 

In there you will see the following:

```
settings:
  username: tableau_server_username
  password: tableau_server_password
  url: https://tableau_server_url
  shorturl: tableau_server_url
  postgres_user: readonly
  postgres_password: password
  postgres_database: workgroup
  postgres_port: 8060
  token: slack_token - get from Slack API
  archive: where you want your workbooks and stuff to live
```

These are imported into each script, so you only have to change it here (should passwords be updated or a token needs rotating).

Each script is contained in it's own virtual environment. This keeps the dependencies from conflicting on your machine, and makes development simpler.

To keep the repo clean, I have not included dependencies. 

To setup, follow [these instructions](http://docs.python-guide.org/en/latest/dev/virtualenvs/).

Once the environment is ready, copy the script and requirements.txt, activate your environment and run:

```
pip install -r requirements.txt
```

That will install all your dependencies and you are ready to go.

---

## What is included:

Some/most of the scripts also have jupyter notebooks so you can see each step in detail, and experiment yourself.

+ Archive: Pull ALL workbooks from Server into a file structure that mirrors your Projects and Sites. 
+ Cleanup: Remove and reassign content from unlicensed users, delete those users, notify for reassignment
+ Content: This is two scripts to tag stale content and subsequently delete it. This will keep your content fresh (h/t to [Mark Jacobsen](https://twitter.com/ViziblyDiffrnt))
+ Remap: If data sources change, or need to be changed between environments, this will remap like-to-like (SQL Server -> SQL Server)
+ Extracts: Dynamically reprioritize Extracts based on historical run history and completion percentages. It's basically `tabadmin set backgrounder.sort_jobs_by_run_time_history_observable_hours` on steroids
+ Permissions: Archive all workbook permissions to Dynamo
+ Sandbox: Compare list of users to list of Sandbox projects and create new ones as needed.
+ Restart: This is how to use SSM to do command-line activities on Tableau Server. It requires you to either be running Tableau Server in AWS, or have a managed instance on-premise. It also recommends (but not requires) you to have tabadmin on path. If you don't know how to do this, you can run tabadmin.js
+ Tabadmin: This is a node script that will accept a Drive letter (D:) and a version number. It will then place tabadmin onto your path so you can call it anywhere on the command line or in scripts directly.
