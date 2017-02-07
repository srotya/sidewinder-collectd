/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.srotya.sidewinder.collectd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.collectd.api.Collectd;
import org.collectd.api.CollectdConfigInterface;
import org.collectd.api.CollectdWriteInterface;
import org.collectd.api.DataSource;
import org.collectd.api.OConfigItem;
import org.collectd.api.OConfigValue;
import org.collectd.api.ValueList;

/**
 * Sidewinder Collectd Writer
 *
 * @author ambud
 */
public class SidewinderCollectdWriter implements CollectdWriteInterface, CollectdConfigInterface {

	private List<String> urls;
	private List<ValueList> batch;
	private int batchSize = 100;

	public SidewinderCollectdWriter() {
		Collectd.registerConfig("SidewinderCollectdWriter", this);
		Collectd.registerWrite("SidewinderCollectdWriter", this);
	}

	public synchronized int write(ValueList valueList) {
		batch.add(valueList);
		if (batch.size() >= batchSize) {
			StringBuilder builder = new StringBuilder();
			for (ValueList val : batch) {
				String src = val.getSource();
				String[] splits = src.split("/");
				String host = splits[0];
				String measurement = splits[1];
				String valueFieldName = splits[splits.length - 1];
				List<String> tags = new ArrayList<>();
				tags.add(host);
				for (int i = 2; i < splits.length - 1; i++) {
					tags.add(splits[i]);
				}
				StringBuilder tagStr = new StringBuilder();
				for (String tag : tags) {
					tagStr.append("," + tag);
				}
				String tagStrCnst = tagStr.toString();
				for (DataSource dataSource : val.getDataSet().getDataSources()) {
					builder.append(measurement + tagStrCnst + " " + valueFieldName + "=" + dataSource.getMin() + " "
							+ val.getTime() + "\n");
				}
			}
//			System.out.println("Sidewinder output:\n" + builder.toString());
			for (String url : urls) {
				CloseableHttpClient client = HttpClientBuilder.create().build();
				HttpPost post = new HttpPost(url);
				try {
					post.setEntity(new StringEntity(builder.toString()));
					CloseableHttpResponse result = client.execute(post);
					if (result.getStatusLine().getStatusCode() < 200 || result.getStatusLine().getStatusCode() > 299) {
						System.err.println("Failed to write batch to " + url + "reason:" + result.getStatusLine());
						return -1;
					}
				} catch (Exception e) {
					System.err.println("Failed to write batch to " + url + "reason:" + e.getMessage());
					return -1;
				} finally {
					try {
						client.close();
					} catch (IOException e) {
						return -1;
					}
				}
//				System.out.println("Returning 0 "+System.currentTimeMillis());
			}
			batch.clear();
		}
		return 0;
	}

	public int config(OConfigItem conf) {
		Set<String> urls = new HashSet<>();
		List<OConfigItem> children = conf.getChildren();
		for (OConfigItem oConfigItem : children) {
			if (oConfigItem.getKey().equalsIgnoreCase("Tags")) {

			} else if (oConfigItem.getKey().equalsIgnoreCase("Connection")) {
				List<OConfigItem> connectionConf = oConfigItem.getChildren();
				for (OConfigItem connectionConfItem : connectionConf) {
					List<OConfigValue> values = connectionConfItem.getValues();
					for (OConfigValue host : values) {
						urls.add(host.getString());
					}
				}
			} else if (oConfigItem.getKey().equalsIgnoreCase("Meta")) {
				List<OConfigItem> meta = oConfigItem.getChildren();
				for (OConfigItem metaItem : meta) {
					switch (metaItem.getKey().toLowerCase()) {
					case "batchsize":
						batchSize = metaItem.getValues().iterator().next().getNumber().intValue();
						break;
					default:
					}
				}
			}
		}

		batch = new ArrayList<>(batchSize);

		if (urls.size() == 0) {
			System.err.println("Bad configuration no Sidewinder DB Urls specified!");
			return -1;
		}
		this.urls = new ArrayList<>(urls);
		return 0;
	}

}
