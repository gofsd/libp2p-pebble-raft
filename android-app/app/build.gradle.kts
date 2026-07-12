plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.gofsd.kvdemo"
    compileSdk = 36

    defaultConfig {
        applicationId = "com.gofsd.kvdemo"
        minSdk = 26 // ASharedMemory_create's minimum (see pkg/ipc/ipc_android.go)
        targetSdk = 36
        versionCode = 1
        versionName = "1.0"
        // Drives E2ETest (see src/androidTest) -- pkg/e2erun's Android
        // execution path runs it via `adb shell am instrument`.
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    buildTypes {
        debug {
            isMinifyEnabled = false
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }

    ndkVersion = "28.2.13676358"
}

dependencies {
    implementation(files("libs/kvmobile.aar"))
    androidTestImplementation(files("libs/kvmobile.aar"))
    androidTestImplementation("androidx.test.ext:junit:1.2.1")
    androidTestImplementation("androidx.test:runner:1.6.2")
}
