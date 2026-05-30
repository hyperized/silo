{{/*
The CSI driver name. This is the external contract: StorageClass.provisioner,
the CSIDriver object name, and the per-node socket directory all key on it, so
it is fixed and not templated from the release name.
*/}}
{{- define "silo-csi.driverName" -}}
csi.silo.hyperized.net
{{- end -}}

{{- define "silo-csi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "silo-csi.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "silo-csi.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "silo-csi.labels" -}}
app.kubernetes.io/name: {{ include "silo-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* The silo-csi image reference, defaulting the tag to the chart appVersion. */}}
{{- define "silo-csi.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/* The kubelet plugin directory for this driver's socket. */}}
{{- define "silo-csi.pluginDir" -}}
{{- printf "%s/plugins/%s" (.Values.node.kubeletDir | trimSuffix "/") (include "silo-csi.driverName" .) -}}
{{- end -}}
