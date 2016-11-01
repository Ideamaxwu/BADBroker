package edu.uci.ics.badproject.badclient;

import android.app.Activity;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class MainActivity extends Activity {
    private static final String TAG = "MainActivity";
    public final static String Preference_TAG = "Badclient.Preference";
    public static final String FCM_INTENT = "edu.uci.ics.badclient.OBTAINED_FCMREGISTRATION_TOKEN";

    private TextView txtViewStatus = null;
    private EditText editText = null;

    BADAndroidClient client;
    BroadcastReceiver receiver = null;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        String brokerUrl = getResources().getString(R.string.broker_url);
        String dataverseName = getResources().getString(R.string.dataverseName);

        client = new MyBADClient(this.getApplicationContext(), brokerUrl, dataverseName);
        Log.d(TAG, "Activity is being created");

        setContentView(R.layout.activity_main);
        txtViewStatus = (TextView)findViewById(R.id.txtViewStatus);
        editText = (EditText)findViewById(R.id.editText);

        if (getIntent().getExtras() != null) {
            for (String key : getIntent().getExtras().keySet()) {
                String value = getIntent().getExtras().getString(key);
                Log.d(TAG, "Key: " + key + " Value: " + value);
            }
            this.onNoticationForNewResultsInChannel(getIntent().getExtras());
        }

    }

    @Override
    protected void onStart() {
        super.onStart();
        registerReceiver(receiver = new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, Intent intent) {
                System.out.println("Received Broadcast " + intent.getAction());
                if (intent.getAction().equals(FCM_INTENT)) {
                    String token = intent.getStringExtra("TOKEN");
                    client.setFCMRegistrationToken(token);
                }
            }
        }, new IntentFilter(FCM_INTENT));
    }

    @Override
    protected void onStop() {
        super.onStop();
        if (receiver != null) unregisterReceiver(receiver);
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        Log.i(TAG, "Activity received a new intent");
        if (intent.getExtras() != null)
            this.onNoticationForNewResultsInChannel(intent.getExtras());
    }

    @Override
    protected void onPause() {
        super.onPause();
    }

    public void registerUser(View v) {
        this.client.register("demoapp", "yusuf", "yusuf@abc.net", "pass");
    }

    public void loginUser(View v) {
        client.login("yusuf", "pass");
    }

    public void subscribeChannel(View v) {
        client.subscribe("nearbyTweetChannel", new JSONArray().put("man"));
    }

    public void onNoticationForNewResultsInChannel(Bundle notification) {
        Log.i(TAG, "New results in Channel " + notification.getString("channelName"));

        String channelName = notification.getString("channelName");
        String userSubscriptionId = notification.getString("userSubscriptionId");
        String channelExecutionTime = notification.getString("channelExecutionTime");
        this.client.fetchResults(channelName, userSubscriptionId, channelExecutionTime);
    }


    class MyBADClient extends BADAndroidClient {

        MyBADClient(Context context, String userName, String dataverseName) {
            super(context, userName, dataverseName);
        }

        @Override
        public void onRegistration(JSONObject result) {
            if (result != null)
                Toast.makeText(MainActivity.this, result.toString(), Toast.LENGTH_SHORT).show();
            else
                Toast.makeText(MainActivity.this, "Registration Failed", Toast.LENGTH_SHORT).show();
        }

        @Override
        public void onLogin(JSONObject result) {
            if (result != null) {
                Toast.makeText(MainActivity.this, result.toString(), Toast.LENGTH_SHORT).show();
                try {
                    if (result.getString("status").equals("success")) {
                        Toast.makeText(MainActivity.this, "Login successful", Toast.LENGTH_SHORT).show();
                    }
                    else {
                        Toast.makeText(MainActivity.this, "Login Failed", Toast.LENGTH_SHORT).show();
                    }
                } catch (JSONException jex) {
                    jex.printStackTrace();
                }
            }
            else {
                Toast.makeText(MainActivity.this, "Login Failed", Toast.LENGTH_SHORT).show();
            }
        }

        @Override
        public void onSubscription(JSONObject result) {
            if (result != null)
                Toast.makeText(MainActivity.this, result.toString(), Toast.LENGTH_SHORT).show();
            else
                Toast.makeText(MainActivity.this, "Subscription Failed", Toast.LENGTH_SHORT).show();
        }

        @Override
        public void onNewResultsRetrieved(JSONObject result) {
            if (result != null)
                Toast.makeText(MainActivity.this, "New results in channel", Toast.LENGTH_SHORT).show();
            else
                Toast.makeText(MainActivity.this, "Getresults Failed", Toast.LENGTH_SHORT).show();

            Log.i(TAG, result != null ? result.toString() : "");
        }
    }
}
