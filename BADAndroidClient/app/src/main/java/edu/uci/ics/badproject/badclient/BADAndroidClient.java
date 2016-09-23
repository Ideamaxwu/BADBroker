package edu.uci.ics.badproject.badclient;

import android.os.AsyncTask;
import android.util.Log;

import com.google.firebase.iid.FirebaseInstanceId;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by ysarwar on 9/22/16.
 */

public abstract class BADAndroidClient {
    public final static String TAG = "BADAndroidClient";
    private String broker = "10.0.2.2";
    private int port = 8989;
    private String brokerUrl = "http://" + this.broker + ":" + this.port;

    private String dataverseName = null;
    public String userName, email, password, userId, accessToken;
    private String gcmRegistrationToken = null;

    public BADAndroidClient() {
    }

    public BADAndroidClient(String broker, int port) {
        this.broker = broker;
        this.port = port;
        this.brokerUrl = "http://" + this.broker + ":" + this.port;
    }

    public void setBroker(String server, int port) {
        this.broker = server;
        this.port = port;
    }

    public void setDataverse(String dv) {
        this.dataverseName = dv;
    }

    public void register(String dataverseName, String userName, String email, String password) {
        this.dataverseName = dataverseName;
        this.userName = userName;
        this.email = email;
        this.password = password;

        JSONObject postData = new JSONObject();

        try {
            postData.put("dataverseName", this.dataverseName);
            postData.put("userName", this.userName);
            postData.put("email", this.email);
            postData.put("password", this.password);

            PostCallTask task = new PostCallTask() {
                @Override
                protected void onPostExecute(String s) {
                    if (s != null) {
                        try {
                            onRegistration(new JSONObject(s));
                        } catch (JSONException jex) {
                            jex.printStackTrace();
                            Log.e(TAG, "Malformatted result " + s);
                            onRegistration(null);
                        }
                    } else {
                        Log.e(TAG, "Empty result is returned");
                        onRegistration(null);
                    }
                }
            };

            task.execute(brokerUrl, "register", postData.toString());

        } catch (JSONException jex){
            jex.printStackTrace();
        }
    }

    public void login(String userName, String password) {
        JSONObject postData = new JSONObject();

        try {
            postData.put("dataverseName", this.dataverseName);
            postData.put("userName", userName);
            postData.put("password", password);
            postData.put("platform", "android");

            this.gcmRegistrationToken = FirebaseInstanceId.getInstance().getToken();
            Log.i(TAG, "Obtained GCM registration token " + this.gcmRegistrationToken);

            if (this.gcmRegistrationToken != null)
                postData.put("gcmRegistrationToken", this.gcmRegistrationToken);
            else
                postData.put("gcmRegistrationToken", "Nil");

            PostCallTask task = new PostCallTask() {
                @Override
                protected void onPostExecute(String s) {
                    if (s != null) {
                        try {
                            JSONObject result = new JSONObject(s);
                            onLogin(result);
                        } catch (JSONException jex) {
                            jex.printStackTrace();
                            Log.e(TAG, "Malformated result " + s);
                            onLogin(null);
                        }
                    } else {
                        Log.e(TAG, "Empty result is returned");
                        onLogin(null);
                    }
                }
            };

            task.execute(brokerUrl, "login", postData.toString());

        } catch (JSONException jex){
            jex.printStackTrace();
        }
    }

    public void subscribe(String channelName, JSONArray parameters) {
        if (this.userId == null || this.accessToken == null) {
            JSONObject result;
            try{
                result = new JSONObject().put("status", "failed").put("error", "Perhaps, User is not logged in");
                onSubscription(result);
            } catch (JSONException jex){
            }
            return;
        }

        JSONObject postData = new JSONObject();

        try {
            postData.put("dataverseName", this.dataverseName);
            postData.put("userId", this.userId);
            postData.put("accessToken", this.accessToken);
            postData.put("channelName", channelName);
            postData.put("parameters", parameters);

            PostCallTask task = new PostCallTask() {
                @Override
                protected void onPostExecute(String s) {
                    if (s != null) {
                        try {
                            onSubscription(new JSONObject(s));
                        } catch (JSONException jex) {
                            jex.printStackTrace();
                            Log.e(TAG, "Malformatted result " + s);
                            onSubscription(null);
                        }
                    } else {
                        Log.e(TAG, "Empty result is returned");
                        onSubscription(null);
                    }
                }
            };

            task.execute(brokerUrl, "subscribe", postData.toString());

        } catch (JSONException jex){
            jex.printStackTrace();
        }
    }

    class PostCallTask extends AsyncTask<String, Void, String> {
        @Override
        protected String doInBackground(String... strings) {
            return postCall(strings[0], strings[1], strings[2]);
        }
    }

    public void fetchResults(String channelName, String userSubscriptionId, String channelExecutionTime) {
        if (this.userId == null || this.accessToken == null) {
            JSONObject result;
            try{
                result = new JSONObject().put("status", "failed").put("error", "Perhaps, User is not logged in");
                onNewResultsRetrieved(result);
            } catch (JSONException jex){
            }
            return;
        }

        JSONObject postData = new JSONObject();

        try {
            postData.put("dataverseName", this.dataverseName);
            postData.put("userId", this.userId);
            postData.put("accessToken", this.accessToken);
            postData.put("channelName", channelName);
            postData.put("userSubscriptionId", userSubscriptionId);
            postData.put("channelExecutionTime", channelExecutionTime);

            PostCallTask task = new PostCallTask() {
                @Override
                protected void onPostExecute(String s) {
                    if (s != null) {
                        try {
                            onNewResultsRetrieved(new JSONObject(s));
                        } catch (JSONException jex) {
                            jex.printStackTrace();
                            Log.e(TAG, "Malformatted result " + s);
                            onNewResultsRetrieved(null);
                        }
                    } else {
                        Log.e(TAG, "Empty result is returned");
                        onNewResultsRetrieved(null);
                    }
                }
            };

            task.execute(brokerUrl, "getresults", postData.toString());

        } catch (JSONException jex){
            jex.printStackTrace();
        }
    }

    // Taken from http://stackoverflow.com/questions/20279195/android-post-request-using-httpurlconnection
    public String postCall(String url, String servicePoint, String params){
        InputStream is = null;
        String response = null;
        HttpURLConnection urlConn = null;

        try {
            URL _url = new URL(url + "/" + servicePoint);
            urlConn =(HttpURLConnection)_url.openConnection();
            urlConn.setRequestMethod("POST");
            urlConn.setRequestProperty("Content-Type", "applicaiton/json; charset=utf-8");
            urlConn.setRequestProperty("Accept", "applicaiton/json");
            urlConn.setDoOutput(true);

            Log.i(TAG, "Making call " + urlConn.toString());
            Log.i(TAG, "params " + params);

            urlConn.connect();

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(urlConn.getOutputStream()));
            writer.write(params);
            writer.flush();
            writer.close();

            if(urlConn.getResponseCode() == HttpURLConnection.HTTP_OK){
                is = urlConn.getInputStream();
            } else {
                is = urlConn.getErrorStream();
            }


        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), 8);
            StringBuilder sb = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
            is.close();
            response = sb.toString();
            Log.d(TAG, response);
        } catch (Exception e) {
            Log.e("Buffer Error", "Error converting result " + e.toString());
        }

        if (urlConn != null) {
            urlConn.disconnect();
        }

        return response;
    }

    public String toString() {
        return this.userId + " -- " + this.accessToken;
    }

    public abstract void onRegistration(JSONObject result);
    public abstract void onLogin(JSONObject result);
    public abstract void onSubscription(JSONObject result);
    public abstract void onNewResultsRetrieved(JSONObject result);
}
