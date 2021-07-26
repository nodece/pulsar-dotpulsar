/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DotPulsar.Internal
{
    using System;

    public class TopicName
    {
        private const string PUBLIC_TENANT = "public";
        private const string DEFAULT_NAMESPACE = "default";
        private const string PARTITIONED_TOPIC_SUFFIX = "-partition-";
        private const string PERSISTENT = "persistent";
        private const string NON_PERSISTENT = "non-persistent";

        private string domain;
        private string tenant;
        private string? cluster;
        private string namespacePortion;
        private string localName;
        private NamespaceName namespaceName;
        private int partitionIndex;
        private string completeTopicName;

        public TopicName(string completeTopicName)
        {
            try
            {
                // The topic name can be in two different forms, one is fully qualified topic name,
                // the other one is short topic name
                string[] parts;

                if (!completeTopicName.Contains("://"))
                {
                    // The short topic name can be:
                    // - <topic>
                    // - <property>/<namespace>/<topic>
                    parts = completeTopicName.Split('/');

                    completeTopicName = parts.Length switch
                    {
                        3 => PERSISTENT + "://" + completeTopicName,
                        1 => PERSISTENT + "://" + PUBLIC_TENANT + "/" + DEFAULT_NAMESPACE + "/" + parts[0],
                        _ => throw new Exception("Invalid short topic name '" + completeTopicName +
                                                 "', it should be in the format of " +
                                                 "<tenant>/<namespace>/<topic> or <topic>")
                    };
                }

                // The fully qualified topic name can be in two different forms:
                // new:    persistent://tenant/namespace/topic
                // legacy: persistent://tenant/cluster/namespace/topic

                parts = completeTopicName.Split(new[] { ':', '/', '/' }, 2, StringSplitOptions.RemoveEmptyEntries);
                domain = PERSISTENT.Equals(parts[0]) ? PERSISTENT : NON_PERSISTENT;

                string rest = parts[1];

                // The rest of the name can be in different forms:
                // new:    tenant/namespace/<localName>
                // legacy: tenant/cluster/namespace/<localName>
                // Examples of localName:
                // 1. some, name, xyz
                // 2. xyz-123, feeder-2

                parts = rest.Split(new[] { ':', '/', '/' }, 4, StringSplitOptions.RemoveEmptyEntries);

                switch (parts.Length)
                {
                    case 3:
                        // New topic name without cluster name
                        tenant = parts[0];
                        cluster = null;
                        namespacePortion = parts[1];
                        localName = parts[2];
                        partitionIndex = GetPartitionIndex(completeTopicName);
                        namespaceName = NamespaceName.Get(tenant, namespacePortion);
                        break;
                    case 4:
                        // Legacy topic name that includes cluster name
                        tenant = parts[0];
                        cluster = parts[1];
                        namespacePortion = parts[2];
                        localName = parts[3];
                        partitionIndex = GetPartitionIndex(completeTopicName);
                        namespaceName = NamespaceName.Get(tenant, cluster, namespacePortion);
                        break;
                    default: throw new Exception("Invalid topic name: " + completeTopicName);
                }

                if (string.IsNullOrEmpty(localName))
                {
                    throw new Exception("Invalid topic name: " + completeTopicName);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Invalid topic name: " + completeTopicName, e);
            }

            this.completeTopicName =
                IsV2()
                    ? $"{domain}://{tenant}/{namespacePortion}/{localName}"
                    : $"{domain}://{tenant}/{cluster}/{namespacePortion}/{localName}";
        }

        private bool IsV2()
        {
            return cluster == null;
        }

        public NamespaceName GetNamespaceName()
        {
            return namespaceName;
        }

        private static string SubstringAfterLast(string str, string separator)
        {
            if (string.IsNullOrEmpty(str))
            {
                return str;
            }

            if (string.IsNullOrEmpty(separator))
            {
                return "";
            }

            var pos = str.LastIndexOf(separator, StringComparison.Ordinal);
            return pos != -1 && pos != str.Length - separator.Length ? str.Substring(pos + separator.Length) : "";
        }

        private static int GetPartitionIndex(string topic)
        {
            var partitionIndex = -1;

            if (!topic.Contains(PARTITIONED_TOPIC_SUFFIX))
                return partitionIndex;

            try
            {
                var idx = SubstringAfterLast(topic, PARTITIONED_TOPIC_SUFFIX);
                partitionIndex = int.Parse(idx);

                if (partitionIndex < 0)
                {
                    // for the "topic-partition--1"
                    partitionIndex = -1;
                }
                else if (idx.Length != partitionIndex.ToString().Length)
                {
                    // for the "topic-partition-01"
                    partitionIndex = -1;
                }
            }
            catch (Exception e)
            {
                // ignore exception
            }

            return partitionIndex;
        }

        public override string ToString()
        {
            return completeTopicName;
        }
    }
}
