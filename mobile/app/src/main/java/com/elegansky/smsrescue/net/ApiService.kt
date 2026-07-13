package com.elegansky.smsrescue.net

import com.squareup.moshi.JsonClass
import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import retrofit2.Response
import retrofit2.Retrofit
import retrofit2.converter.moshi.MoshiConverterFactory
import retrofit2.http.Body
import retrofit2.http.POST
import java.util.concurrent.TimeUnit

@JsonClass(generateAdapter = true)
data class SmsRescueRequest(val message: String)

@JsonClass(generateAdapter = true)
data class SmsRescueResponse(
    val rescued: Boolean = false,
    val row_id: Long? = null,
    val source_tab: String? = null,
    val plate: String? = null,
    val ref: String? = null,
    val error: String? = null,
    val message: String? = null,
)

interface SmsRescueApi {
    @POST("api/sms-rescue")
    suspend fun rescue(@Body body: SmsRescueRequest): Response<SmsRescueResponse>
}

object ApiFactory {

    fun build(baseUrl: String, token: String): SmsRescueApi {
        val moshi = Moshi.Builder().add(KotlinJsonAdapterFactory()).build()
        val tokenInterceptor = Interceptor { chain ->
            val req = chain.request().newBuilder()
                .addHeader("X-Migration-Token", token)
                .build()
            chain.proceed(req)
        }
        val client = OkHttpClient.Builder()
            .addInterceptor(tokenInterceptor)
            .addInterceptor(HttpLoggingInterceptor().apply {
                level = HttpLoggingInterceptor.Level.BASIC
            })
            .connectTimeout(20, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build()
        return Retrofit.Builder()
            .baseUrl(baseUrl.trimEnd('/') + "/")
            .client(client)
            .addConverterFactory(MoshiConverterFactory.create(moshi))
            .build()
            .create(SmsRescueApi::class.java)
    }
}
