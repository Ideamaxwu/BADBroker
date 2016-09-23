package edu.uci.ics.badproject.badclient;

import android.content.SharedPreferences;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class MainActivity extends AppCompatActivity {
    private static final String TAG = "MainActivity";

    public static final String PREFS = "Prefs";
    private TextView txtViewStatus = null;
    private EditText editText = null;

    BADAndroidClient client = new MyBADClient("channels");

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
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
    }

    @Override
    protected void onStop() {
        super.onStop();
    }

    public void registerUser(View v) {
        this.client.register("channels", "yusuf", "yusuf@abc.net", "pass");
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
        public MyBADClient(String dataverseName) {
            super.setDataverse(dataverseName);
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
                        client.userId = result.getString("userId");
                        client.accessToken = result.getString("accessToken");
                        

                    }
                } catch (JSONException jex) {

                }

            }
            else
                Toast.makeText(MainActivity.this, "Login Failed", Toast.LENGTH_SHORT).show();
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

            Log.i(TAG, result.toString());
        }
    }
}
