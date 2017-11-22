# Tableau Conference 2017
If you attended my session, watched it on Tableau Live, heard about it, or are just trolling GitHub for Tableau Server-related stuff, this is the repo for you!

In case you haven't seen the presentation - it can be found [here](https://www.dropbox.com/s/pcbj7bcbualajjj/Zillow%20TC17.pptx?dl=0)

Everything you see here was developed during my time at Zillow, and is in use there. 

In instances where the workbooks or scripts need explanation, it will be provided. In the cases of the workbooks, you will likely need to swap out your own content instead of my placeholders (which are there so nothing proprietary or protected is shared).

I'm happy to discuss the content, but since I'm no longer at Zillow, issues will likely not be addressed with new code. Pull Requests are welcome!

---

The Style-Guide document is included in the top level, for the rest I've segmented the Scripts into the `Scripts` folder, and Workbooks into the `Workbooks` folder. Please see those for specific instructions.

Note: All but one piece of automation is written in Python2, the other one is written in Node.

---

# Deployment

You can deploy these any way you want. They were designed to fit under the 5m AWS Lambda timeout. We didn't always hit it, but if you parallelize it enough and/or throw enough memory at it, you might get there too. For huge Server installs, you'll need a separate box.

Since Tableau Server runs on Windows, which doesn't always play well with Python, we deployed these on a Ubuntu box that could talk to Tableau Server. We installed all dependencies globally since the bo is single purpose. However, all scripts have their individual dependencies and were designed using `virtualenv`. You can deploy them that way if you like, just don't forget to activate before running. 

Our scheduling service is Cron. You can also use AWS SSM, which is how we do all regularly scheduled `tabadmin` work.

## notes on AWS

AWS is a consistent theme in the presentation. It's in heavy rotation at Zillow, however, it may not be for you or your company. That's cool. SSM (if you choose to use it), doesn't require your systems to be in the cloud and you incur a total cost of $0 if you use it. 

There's one Lambda function that invokes SSM, but you could schedule that with CRON too, if you don't want to pay the $.02 invocation cost. 

Permissions are stored in Dynamo - mainly because it's easy. You can use whatever you want, but it needs to understand JSON natively. Our Dynamo is configured with 1 Read and 10 Write (since we write more than we read). Total cost: $4.94/month.