@Grab("com.squareup.okhttp3:okhttp:3.13.1")
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody

import groovy.json.JsonOutput
import groovy.transform.Field
import okhttp3.MediaType

@Field
OkHttpClient client = new OkHttpClient()

@Field
Random random = new Random()

int x = 100

x.times {
    post(
        group: "6735",
        localUrl: "http://1.1.1.$it",
        offset: (int)Math.sqrt(x - it) * 10,
        status: "following"
    )
}

(x/2 - 3).times {
    post(
        group: "6735",
        localUrl: "http://1.1.1.${x+it}",
        offset: (int)Math.sqrt(x - it) * 10,
        status: randomStatus()
    )
}

def post(Map body) {
    post(JsonOutput.toJson(body))
}

def post(String body) {
    Request request = new Request.Builder()
        .url('http://localhost:8080/registry')
        .post(RequestBody.create(MediaType.get("application/json"), body))
        .build()

    println client.newCall(request).execute()
}

def randomStatus() {
    def x = random.nextInt(100)
    if (x < 37) return "stale"
    return "initialising"
}