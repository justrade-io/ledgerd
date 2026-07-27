rootProject.name = "adbe"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("adbe-protocol")
include("adbe-core")
include("adbe-launcher")
include("adbe-client")
include("adbe-tests")
include("adbe-examples")
