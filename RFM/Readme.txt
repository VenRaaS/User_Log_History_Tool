�{���X�ϥλ���:
(�аѷ� RFM_���c�� )

1.RFM_relabel.sql: ��s rfm_label_table

2.RFM_count_script.sql: �Hrfm_label_table�����s����ǡA�p���ѦU�s���W�u�H��
                        �æs�JDB.rfm_123_daily_record��table���C

3.RFM_hr_script.sql:    �Hrfm_label_table�����s����ǡA�p��U�s��Ѫ��I���v
                        �æs�JDB.rfm_123_hr_record��table���C

4.RFM_monthly_relabel.sh: ����RFM_relabel.sql�A�I�bcrontab���A�@�Ӥ����@���C

5.RFM_daily_script.sh: �P�ɱ���RFM_count_script.sql + RFM_hr_script.sql
                       �I�bcrontab���A�@�Ѱ���@���C

6.RFM_generate_many_days.sh: �妸����shell�����ɡA�i�ۤv�s��n�p�⪺�~�B��B��A
                             �P�ɱ���RFM_count_script.sql + RFM_hr_script.sql�A
                             �q�`�Ω󱼸�Ʈɻݭn�ɸ�ƪ��ɭԡC

-------------------------------------------------------------------------------------

########### ��ʧ�s DB.rfm_label_table ###########

step-1.�s��generate_many_days.sh�ɡA�䥦�����ѱ��A�u�d�U:
       hive -f RFM/RFM_relabel.sql -hiveconf YEAR=2017 -hiveconf MONTH=06 -hiveconf DATE=01;
       
step-2.�blinux command line ��J:

	source RFM/generate_many_days.sh

step-3.press "enter"�A�N�i�H�o��H6/1����¦��X�Ӫ� RFM���stable�F�C

########### ���insert��ƶitable ###########

step-1.�ק�generate_many_days.sh�ɡA�[�J�Q�ninsert�� RFM record�����

step-2.�blinux command line ��J:

	source RFM/generate_many_days.sh

step-3.press "enter"

########### �]�mcrontab�۰ʤ� ###########

step-1.�bRFM_daily_script.sh����DATE=$(date +"%Y%m%d" --date="1 days ago")���X�Q�Ѥ��
       �A�z�L������O���᭱�[�W -hiveconf �ǤJDATE�ܼ�
       ������O��file����${hiveconf:DATE}�����ܼ�

step-2.��crontab�[�J�Ƶ{����k:
     (1)��crontab -e �s��Ƶ{
     (2)�A��i�i�Jvim���s��Ҧ�
     (3)�s�觹���esc
     (4)���ۿ�J:wq ��enter (���write & quit)

*�z�Lcrontab -l �i�H�ݥثe�����Ƶ{

step-3.�]�w�Ƶ{���榡
	ex:
        	20 15 * * * source home/W100.ITRI/u453041/daily_shell_script.sh 
		 ^  ^	^     ^       ^
	���:�b	�� ��  any   ����   script    
        	        ��
  			��
  	         (�ѡB�g�B��)



