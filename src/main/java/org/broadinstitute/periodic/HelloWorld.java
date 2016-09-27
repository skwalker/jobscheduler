package org.broadinstitute.periodic;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.impl.matchers.GroupMatcher.groupEquals;
import static spark.Spark.*;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HelloWorld {

    // ADD A JOB
    // curl -d "{\"intervalSeconds\":4, \"jobName\":\"test1\", \"HttpMethod\":\"POST\", \"requestBody\": {\"hello\": \"there\"}, \"url\":\"http://requestb.in/rnruc0rn\" }" http://localhost:4567/job

    // DELETE A JOB
    // curl -X delete -d "{ \"jobName\":\"test4\"}"  http://localhost:4567/job

    // GET LIST OF JOBS:
    // curl -X get http://localhost:4567/jobs


    // TODO: LATER
    // need to improve error handling and log messages
    // ESPECIALLY handle bad inputs better (immediately get 500 error... groan)

    private static final String groupName = "group1";

    public static void main(String[] args) {
        JsonParser parser = new JsonParser();

        Logger logger = LoggerFactory.getLogger(HelloWorld.class);

        try {
            // Grab the Scheduler instance from the Factory
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

            // and start it off
            scheduler.start();

        // add a job
        post("/job", (req, res) -> {
            JsonObject obj = parser.parse(req.body()).getAsJsonObject();
            int seconds = obj.get("intervalSeconds").getAsInt();
            String name = obj.get("jobName").getAsString();
            String method = obj.get("HttpMethod").getAsString();
            JsonObject body = obj.get("requestBody").getAsJsonObject();
            String url = obj.get("url").getAsString();

            try {
                JobDetail job = newJob(HttpJob.class)
                        .withIdentity(name, groupName)
                        .usingJobData("httpMethod", method)
                        .usingJobData("requestBody", body.toString())
                        .usingJobData("httpURL", url)
                        .build();
                Trigger trigger = newTrigger()
                        .withIdentity("trigger" + name, groupName)
                        .startNow()
                        .withSchedule(simpleSchedule()
                                .withIntervalInSeconds(seconds)
                                .repeatForever())
                        .build();

                scheduler.scheduleJob(job, trigger);

                return ("Started job " + name);

            } catch (Exception e) {
                logger.info("Error starting job \n" + e.toString());
                return (e.getMessage());
            }

        });

        delete("/job", (req, res) -> {
            try {
                JsonObject request = parser.parse(req.body()).getAsJsonObject();
                String name = request.get("jobName").getAsString();


                for (JobKey jobKey : scheduler.getJobKeys(groupEquals(groupName))) {
                    if (jobKey.getName().equals(name)) {
                        scheduler.deleteJob(jobKey(name, "group1"));
                        return ("Deleted job " + name);
                    }
                }
                logger.info("Error deleting job " + name + " ; job does not exist.");
                return ("Job " + name + " does not exist, could not be deleted.");

            } catch (Exception e) {
                // most likely will be hit if couldn't parse the request
                logger.info("Error deleting job \n" + e.toString());
                return ("Error deleting job " + e.toString());
            }
        });

        get("/jobs", (req, res) -> {

            ArrayList<String> jobInfoList = new ArrayList();

            for(JobKey jobKey : scheduler.getJobKeys(groupEquals(groupName))) {
                JobDataMap dataMap = scheduler.getJobDetail(jobKey).getJobDataMap();
                ArrayList<String> infoList = new ArrayList();  // name, "--" , method, url, "with body", body
                infoList.add(jobKey.getName());
                infoList.add(dataMap.getString("httpMethod"));
                infoList.add(dataMap.getString("httpURL"));
                infoList.add(dataMap.getString("requestBody"));
                infoList.add(1, "--");
                infoList.add(4, "with body");
                String info = String.join(" ", infoList);
                jobInfoList.add(info);
            }

            // example would look like: ["test4 -- PUT http://requestb.in/rnruc0rn with body {\"hello\":\"there\"}"]

            return(new Gson().toJson(jobInfoList));

        });

        } catch (SchedulerException se) {
            logger.info("Got a scheduler exception: " + se.getMessage());
            se.printStackTrace();
        }
    }
}