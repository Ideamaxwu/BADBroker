<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->

# Asterix: Using Channels #

## <a id="toc">Table of Contents</a> ##

* [Broker Management](#Brokers)
* [Channel Management](#Channels)
* [Subscription Management](#Subscriptions)
* [Getting Channel Results](#Results)


This document provides user-facing implementation of Channels over AsterixDB.

## <a id="Brokers">Broker Management</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

This section shows the language for creating/updating Broker information. 

### create broker ###
 * Syntax:

        create broker brokerName at "endPointURL"

 * Enables a new broker as a subscription/notification endPoint
 * Arguments:
    * `brokerName`: A unique `string` name for the broker.
    * `endPointURL`: A `string` value representing the HTTP endPoint for reaching the Broker.

 * Example:

        create broker BrokerA at "http://brokerALocation/notifications"


### drop broker ###
 * Syntax:

        drop broker brokerName ( "if" "exists" )?

 * Removes all information for the given Broker. Fails if there are active subscriptions for that broker
 * Arguments:
    * `brokerName`: A `string` value.

 * Example:

        drop broker BrokerA


### move broker ###
 * Syntax:

        move broker brokerName to "endPointURL"

 * Changes the subscription/notification endPoint for the Broker
 * Arguments:
    * `brokerName`: A `string` value.
    * `endPointURL`: A `string` value.

 * Example:

        move broker BrokerA to "http://brokerALocation/notify"

 
## <a id="Channels">Channel Management</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

This section shows the language for managing Channels. An existing channel will continue to produce <a href="#Results">results</a> for all active <a href="#Subscriptions">subscriptions</a>, and deliver notifications to the endPointURLs for Brokers holding those subscriptions.

### create function for a channel ###
 * See AsterixDB documentation for full details on function creation

 * Example:

        create function NearbyTweets($location, $text) {
  		 for $tweet in dataset TweetMessageuuids
	     let $circle := create-circle($location,30.0)
	     where contains($tweet.message-text,$text)
	     and spatial-intersect($tweet.sender-location, $location)
	     return $tweet.message-text 
        };

 * This function takes two arguments, $location (A `Point`), and $text (A `string`). It scans the dataset TweetMessageuuids, and returns all tweets that are within distance 30 of $location and contain text $text.

### create repetitive channel ###
 * Syntax:

        create repetitive channel channelName using functionSignature period duration("durationValue")

 * Creates the execution engine and datasets for a repetitive channel
 * Arguments:
    * `channelName`: A qualified name for the Channel.
    * `functionSignature`: The signature for the function used by the channel, including the name of the function and the number of arguments
    * `durationValue`: An instance of an Asterix duration value

 * Example:

        create repetitive channel nearbyTweetChannel using NearbyTweets@2 period duration("PT10S");

 * Creates a repetitive channel that will execute every 10 seconds. This channel will use the function from above. Subscriptions to this channel will need to include values for these two arguments ($location and $text). Results will include tuples for which the function is true.

### create continuous channel ###
 * Syntax:

        create continuous channel channelName using functionSignature

 * Creates the execution engine and datasets for a continuous channel
 * Arguments:
    * `channelName`: A qualified name for the Channel.
    * `functionSignature`: The signature for the function used by the channel, including the name of the function and the number of arguments

 * Example:

        create continuous channel nearbyTweetChannel using NearbyTweets@2;

 * Creates a continous channel that will execute on changes to the data. This channel will use the function from above. Subscriptions to this channel will need to include values for these two arguments ($location and $text). Results will include tuples for which the function is true.


### drop channel ###
 * Syntax:

        drop channel channelName

 * Removes the data and execution engine for the channel
 * Arguments:
    * `channelName`: The qualified name of the channel.

 * Example:

        drop channel nearbyTweetChannel



## <a id="Subscriptions">Subscription Management</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

This section shows the language for managing Subscriptions.

### subscribe ###
 * Syntax:

        subscribe to channelName (parameterList) on brokerName;

 * Creates a parameterized subscription to the existing channel. Future channel execution will produce results for this subscription.
 * Arguments:
    * `channelName`: The Qualified name for the Channel.
    * `parameterList`: A list of the parameter values for this subscription
    * `brokerName`: A `string` representing the broker hosting this subscription.
 * Return Value:
    * A `uuid` representing the id of the new subscription

 * Example:

        subscribe to nearbyTweetChannel (point("30.0, 30.0"), "Live") on brokerA;

 * The expected result is:
 
        uuid("76252ed5-23c8-4c02-8225-dc6c370c7556") 
        
 * This id can be used when the broker retrives <a href="#Results">results</a>.


### unsubscribe ###
 * Syntax:

        unsubscribe "uuidString" from channelName;

 * Removes the data and execution engine for the channel
 * Arguments:
    * `uuidString `: A `string` value representing the uuid of the subscription.
    * `channelName`: A `string` value representing the name of the channel.

 * Example:

        unsubscribe "c45ef6d0-c5ae-4b9e-b5da-cf1932718296" from nearbyTweetChannel;
        
### move subscription###
 * Syntax:

        move subscription "uuidString" on channelName to brokerName

 * Removes the data and execution engine for the channel
 * Arguments:
    * `uuidString `: A `string` value representing the uuid of the subscription.
    * `channelName`: The qualified name for the Channel.
    * `brokerName`: A `string` representing the new broker to host this subscription.

 * Example:

        move subscription "c45ef6d0-c5ae-4b9e-b5da-cf1932718296" on nearbyTweetChannel to BrokerB

## <a id="Results">Getting Channel Results</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

This section shows the language for getting channel results. Brokers will be notified when new results are produced. The results dataset can be queried as an AsterixDB dataset. It is up to the broker to decide when to fetch results. For more on querying datasets, refer to the AsterixDB documentation.

### read results ###
 * Syntax:

        for $result in dataset channelNameResults

 * The name of the results table is simply the channelName appended with "Results"
 * Arguments:
    * `channelName`: A `string` representing the name for the Channel.
 * Return Value:
    * Results as specified by the query
    * Result format is based on return clause of channel function

 * Example:

        for $result in dataset nearbyTweetChannelResults
        where $result.subscriptionId = uuid("76252ed5-23c8-4c02-8225-dc6c370c7556") 
        and $result.deliveryTime >= datetime("2011-12-26T10:10:00.000Z")
        return $result.result;

 * The expected result is:
 
        {"message-text":"Hello World Live"}
        {"message-text":"Live Long and Prosper"}
        
        
### delete results ###

 * Example:

        delete $result in dataset nearbyTweetChannelResults
        where $result.deliveryTime <= datetime("2011-12-26T10:10:00.000Z");

### REST API ###

#### Select query with synchronous result delivery ####


        use dataverse channels;
        
        for $result in dataset nearbyTweetChannelResults
        where $result.subscriptionId = uuid("76252ed5-23c8-4c02-8225-dc6c370c7556")
        return $result.result;


API call for the above query statement in the URL-encoded form.

[http://localhost:19002/query?query=use%20dataverse%20 channels;for%20$result%20in%20dataset%20 nearbyTweetChannelResults%20where%20$result.subscriptionId%20=%20uuid("76252ed5-23c8-4c02-8225-dc6c370c7556")%20return%20$result;](http://localhost:19002/query?query=use%20dataverse%20 channels;for%20$result%20in%20dataset%20 nearbyTweetChannelResults%20where%20$result.subscriptionId%20=%20uuid("76252ed5-23c8-4c02-8225-dc6c370c7556")%20return%20$result;)

#### Response ####
*HTTP OK 200*  
Payload


        {
          "results": [
              [
                  "{"message-text":"Hello World Live"}",
                  "{"message-text":"Live Long and Prosper"}"
              ]
          ]
        }


#### Same select query with asynchronous result delivery ####

API call for the above query statement in the URL-encoded form with mode=asynchronous

[http://localhost:19002/query?query=use%20dataverse%20 channels;for%20$result%20in%20dataset%20 nearbyTweetChannelResults%20where%20$result.subscriptionId%20=%20uuid("76252ed5-23c8-4c02-8225-dc6c370c7556")%20return%20$result;&amp;mode=asynchronous](http://localhost:19002/query?query=use%20dataverse%20 channels;for%20$result%20in%20dataset%20 nearbyTweetChannelResults%20where%20$result.subscriptionId%20=%20uuid("76252ed5-23c8-4c02-8225-dc6c370c7556")%20return%20$result;&amp;mode=asynchronous)

#### Response ####
*HTTP OK 200*  
Payload


        {
            "handle": [45,0]
        }





