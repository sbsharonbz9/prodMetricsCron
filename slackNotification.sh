curl -F file=@output_$today.xls -F filetype=xls -F "initial_comment=Daily metric verifications" -F channels=<redacted> -H "Authorization: Bearer <redacted>" https://slack.com/api/files.upload

