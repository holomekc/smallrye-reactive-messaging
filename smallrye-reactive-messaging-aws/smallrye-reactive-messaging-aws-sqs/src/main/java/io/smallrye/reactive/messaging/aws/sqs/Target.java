package io.smallrye.reactive.messaging.aws.sqs;

import java.util.Objects;

public class Target {

    private final String targetName;
    private final String targetUrl;

    public Target(String targetName, String targetUrl) {
        this.targetName = targetName;
        this.targetUrl = targetUrl;
    }

    public String getTargetName() {
        return targetName;
    }

    public String getTargetUrl() {
        return targetUrl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Target target = (Target) o;
        return Objects.equals(targetUrl, target.targetUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetUrl);
    }
}