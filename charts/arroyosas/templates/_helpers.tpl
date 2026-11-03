{{- define "arroyosas.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "arroyosas.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "arroyosas.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "arroyosas.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "arroyosas.selectorLabels" -}}
app.kubernetes.io/name: {{ include "arroyosas.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "arroyosas.tiledUri" -}}
{{- if .Values.lseOperator.tiled.uri -}}
{{- .Values.lseOperator.tiled.uri -}}
{{- else if .Values.tiled.enabled -}}
{{- printf "http://%s-tiled:%v" (include "arroyosas.fullname" .) .Values.tiled.service.port -}}
{{- else -}}
{{- fail "lseOperator.tiled.uri must be set when tiled.enabled is false" -}}
{{- end -}}
{{- end -}}

{{/* Secret holding the bundled tiled's API key. */}}
{{- define "arroyosas.tiled.secretName" -}}
{{- default (printf "%s-tiled" (include "arroyosas.fullname" .)) .Values.tiled.existingSecret -}}
{{- end -}}

{{- define "arroyosas.tiled.secretKey" -}}
{{- if .Values.tiled.existingSecret -}}
{{- .Values.tiled.existingSecretKey -}}
{{- else -}}
api-key
{{- end -}}
{{- end -}}

{{/*
Bundled tiled API key: tiled.apiKey if set, otherwise the key already stored
in the release's Secret, otherwise a newly generated one.
*/}}
{{- define "arroyosas.tiled.apiKey" -}}
{{- if .Values.tiled.apiKey -}}
{{- .Values.tiled.apiKey -}}
{{- else -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (include "arroyosas.tiled.secretName" .) -}}
{{- if and $existing (index $existing.data "api-key") -}}
{{- index $existing.data "api-key" | b64dec -}}
{{- else -}}
{{- randAlphaNum 32 -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Secret holding the key the operator uses for tiled. Empty when the operator
talks to an external tiled without a key.
*/}}
{{- define "arroyosas.lseOperator.tiledSecretName" -}}
{{- if .Values.lseOperator.tiled.existingSecret -}}
{{- .Values.lseOperator.tiled.existingSecret -}}
{{- else if .Values.lseOperator.tiled.apiKey -}}
{{- printf "%s-lse-operator-tiled" (include "arroyosas.fullname" .) -}}
{{- else if not .Values.lseOperator.tiled.uri -}}
{{- include "arroyosas.tiled.secretName" . -}}
{{- end -}}
{{- end -}}

{{- define "arroyosas.lseOperator.tiledSecretKey" -}}
{{- if .Values.lseOperator.tiled.existingSecret -}}
{{- .Values.lseOperator.tiled.existingSecretKey -}}
{{- else if .Values.lseOperator.tiled.apiKey -}}
api-key
{{- else -}}
{{- include "arroyosas.tiled.secretKey" . -}}
{{- end -}}
{{- end -}}
