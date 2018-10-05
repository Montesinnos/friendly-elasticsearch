package com.montesinnos.friendly.elasticsearch.commons;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import java.io.IOException;
import java.net.URL;
import java.util.List;

public class ResourceUtils {

    public List<String> readLines(final String path) {
        try {
            return Resources.readLines(getUrl(path), Charsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String readStringContent(final String path) {
        try {
            return Resources.toString(getUrl(path), Charsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public URL getUrl(final String path) {
        final URL url = Resources.getResource(path);
        return url;
    }
}
