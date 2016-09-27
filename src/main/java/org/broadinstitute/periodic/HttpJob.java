package org.broadinstitute.periodic;

import org.quartz.*;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by skwalker on 9/23/16.
 */
public class HttpJob implements Job {

    public void execute(JobExecutionContext context)
            throws JobExecutionException {

        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        String httpURL= dataMap.getString("httpURL");
        String bodyString = dataMap.getString("requestBody");
        String method = dataMap.getString("httpMethod");

        try {
            URL url = new URL(httpURL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // prevent 403 error
            connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");

            connection.setDoOutput(true);
            connection.setInstanceFollowRedirects(false); // not sure if this is necessary
            connection.setRequestMethod(method);
            connection.setRequestProperty("Content-Type", "text/json");

            // send body over
            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            wr.writeBytes(bodyString);
            wr.flush();
            wr.close();

            connection.connect();

            int responseCode = connection.getResponseCode();
            System.out.println("\nSending " + method + " request to URL : " + url);
            System.out.println("Response Code: " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            System.out.println(response.toString());

        } catch (IOException e) {
            System.err.println("Fatal transport error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
