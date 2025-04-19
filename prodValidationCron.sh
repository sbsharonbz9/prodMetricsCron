rm $HOME/prodMetricsCron/*.xls
rm $HOME/prodMetricsCron/*.csv
py $HOME/prodMetricsCron/ValidateProdMetricsAll.py
# $HOME/prodMetricsCron/slackNotification.sh
